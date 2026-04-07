import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import time
import re
import unicodedata

# --- CONFIGURAÇÕES DO NEO4J ---
URI = "neo4j://127.0.0.1:7687"
USER = "neo4j"
PASSWORD = "senha111"

def pre_processar(texto):
    if not texto:
        return ""
    # Converte para maiúsculas e remove acentos
    texto = texto.upper()
    texto = ''.join(c for c in unicodedata.normalize('NFKD', texto)
                  if unicodedata.category(c) != 'Mn')
    # Mantém apenas letras, números e barras (importante para siglas como IC/UFF)
    texto = re.sub(r'[^A-Z0-9\s/]', ' ', texto)
    return ' '.join(texto.split())

def normalizar_instituicao(nome_bruto):
    nome = pre_processar(nome_bruto)
    
    if not nome or any(x in nome for x in ["NAO INFORMADA", "NO AFFILIATION", "INSTITUICAO NAO", "DECLARED"]):
        return "Instituição Não Informada"

    # LISTA DE MAPEAMENTO COM ORDEM DE PRIORIDADE
    # As regras mais específicas (como a UFF) devem vir ANTES das genéricas (como Rio de Janeiro)
    mapeamento_prioritario = [
        (r"FLUMIN|UFF|INFES/UFF|IC/UFF|PPGCI/UFF", "Universidade Federal Fluminense (UFF)"),
        
        (r"SAO PAULO|USP|ICMC|POLITECNICA|EACH", "Universidade de São Paulo (USP)"),
        -
        (r"MINAS GERAIS|UFMG|BIOINFORMATICA|DCC/UFMG", "Universidade Federal de Minas Gerais (UFMG)"),
        
        # --- UFRJ (Depois da UFF para não roubar "Fluminense do Rio de Janeiro") ---
        (r"RIO DE JANEIRO|UFRJ|COPPE|PPGI/UFRJ|BNDES|IBCCF|PPGEN|CISI/COPPE", "Universidade Federal do Rio de Janeiro (UFRJ)"),
        
        # --- OUTRAS ---
        (r"CAMPINA GRANDE|UFCG|CAMINA GRANDE", "Universidade Federal de Campina Grande (UFCG)"),
        (r"CAMPINAS|UNICAMP|CEPAGRI", "Universidade Estadual de Campinas (UNICAMP)"),
        (r"CEARA|UFC", "Universidade Federal do Ceará (UFC)"),
        (r"PERNAMBUCO|UFPE|CIN/UFPE|UFRPE|AGRESTE", "Universidade Federal de Pernambuco (UFPE)"),
        (r"SAO CARLOS|UFSCAR", "Universidade Federal de São Carlos (UFSCar)"),
        (r"PUC RIO|PONTIFICAL CATHOLIC UNIVERSITY OF RIO DE JANEIRO", "PUC-Rio"),
        (r"GOIAS|UFG", "Universidade Federal de Goiás (UFG)"),
        (r"PARANA|UFPR", "Universidade Federal do Paraná (UFPR)"),
        (r"UBERLANDIA|UFU", "Universidade Federal de Uberlândia (UFU)"),
        (r"OURO PRETO|UFOP", "Universidade Federal de Ouro Preto (UFOP)"),
        (r"SANTA CATARINA|UFSC", "Universidade Federal de Santa Catarina (UFSC)"),
        (r"RIO GRANDE DO SUL|UFRGS", "Universidade Federal do Rio Grande do Sul (UFRGS)"),
        (r"AMAZONAS|UFAM", "Universidade Federal do Amazonas (UFAM)"),
        (r"BRASILIA|UNB", "Universidade de Brasília (UnB)"),
        (r"SAO JOAO DEL REI|UFSJ", "Universidade Federal de São João Del Rei (UFSJ)"),
        (r"FORTALEZA|UNIFOR", "Universidade de Fortaleza (UNIFOR)"),
        (r"INSTITUTO FEDERAL|IFSP|IFMG|IFPR|IFNMG|IFPB|IFRS|IFSUL|IF SUDOESTE", "Institutos Federais (IFs)"),
        (r"CEFET", "CEFET"),
        (r"UTFPR|TECHNOLOGY PARANA", "UTFPR"),
        (r"AERONAUTICA|ITA", "Instituto Tecnológico de Aeronáutica (ITA)"),
        (r"FIOCRUZ|OSWALDO CRUZ", "Fundação Oswaldo Cruz (Fiocruz)"),
        (r"EMBRAPA", "Embrapa"),
        (r"IBM", "IBM Research"),
        (r"JUSBRASIL", "Jusbrasil"),
    ]

    for padrao, nome_correto in mapeamento_prioritario:
        if re.search(padrao, nome):
            return nome_correto
            
    return nome

def extrair_dados(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        titulo_tag = soup.find("h1", class_="page_title")
        if not titulo_tag: return None
        titulo = titulo_tag.get_text(strip=True)

        data_div = soup.find("div", class_="item published") or soup.find("section", class_="item published")
        data_pub = data_div.find("span", class_="value").get_text(strip=True) if data_div and data_div.find("span", class_="value") else "Data Desconhecida"

        volume_tag = soup.find("section", class_="item issue") or soup.find("div", class_="item issue")
        volume_info = volume_tag.find("a", class_="title").get_text(strip=True) if volume_tag and volume_tag.find("a", class_="title") else "Volume Indefinido"

        autores_lista = []
        autores_tags = soup.select('ul.authors_list > li') or soup.select('.authors li')
        for li in autores_tags:
            nome_tag = li.find('span', class_='name') or li.find('strong')
            if nome_tag:
                nome = nome_tag.get_text(strip=True)
                inst_tag = li.find('span', class_='affiliation')
                inst_original = inst_tag.get_text(strip=True) if inst_tag else "Instituição não informada"
                inst_corrigida = normalizar_instituicao(inst_original)
                autores_lista.append({"nome": nome, "inst": inst_corrigida})

        doi_div = soup.find("div", class_="item doi") or soup.find("section", class_="item doi")
        doi = doi_div.find("a").get_text(strip=True).replace("https://doi.org/", "") if doi_div and doi_div.find("a") else "Sem DOI"

        kw_div = soup.find("section", class_="item keywords") or soup.find("div", class_="item keywords")
        keywords = []
        if kw_div:
            kw_val = kw_div.find("span", class_="value")
            if kw_val:
                keywords = [k.strip().capitalize() for k in kw_val.get_text(strip=True).replace('.', '').split(',')]

        return {
            "titulo": titulo, "data": data_pub, "doi": doi, "volume": volume_info,
            "autores": autores_lista, "keywords": keywords, "url": url
        }
    except Exception as e:
        print(f"⚠️ Erro ao acessar {url}: {e}")
        return None

def salvar_no_neo4j(tx, dados):
    tx.run("MERGE (a:Artigo {titulo: $titulo}) SET a.url = $url", titulo=dados['titulo'], url=dados['url'])
    tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (d:Data {valor: $data}) MERGE (a)-[:PUBLICADO_EM]->(d)", 
           titulo=dados['titulo'], data=dados['data'])
    tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (v:Volume {nome: $vol}) MERGE (a)-[:PERTENCE_AO_VOLUME]->(v)", 
           titulo=dados['titulo'], vol=dados['volume'])

    if dados['doi'] != "Sem DOI":
        tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (id:DOI {codigo: $doi}) MERGE (a)-[:IDENTIFICADO_POR]->(id)", 
                titulo=dados['titulo'], doi=dados['doi'])

    for autor in dados['autores']:
        tx.run("""
            MATCH (a:Artigo {titulo: $titulo})
            MERGE (au:Autor {nome: $nome})
            MERGE (i:Instituicao {nome: $inst})
            MERGE (au)-[:ESCREVEU]->(a)
            MERGE (au)-[:AFILIADO_A]->(i)
        """, titulo=dados['titulo'], nome=autor['nome'], inst=autor['inst'])

    for kw in dados['keywords']:
        tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (k:PalavraChave {nome: $kw}) MERGE (a)-[:TEM_ASSUNTO]->(k)", 
               titulo=dados['titulo'], kw=kw)

if __name__ == "__main__":
    try:
        with open("links_artigos.txt", "r") as f:
            linhas = f.readlines()
    except FileNotFoundError:
        print("❌ Erro: arquivo não encontrado.")
        exit()
    
    urls_para_processar = [l.strip() for l in linhas if l.startswith("http")]
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    with driver.session() as session:
        for i, url in enumerate(urls_para_processar):
            print(f"[{i+1}/{len(urls_para_processar)}] 🔍 Processando: {url}")
            dados = extrair_dados(url)
            if dados:
                session.execute_write(salvar_no_neo4j, dados)
            time.sleep(1)

    driver.close()
    print("\n🚀 Grafo populado com sucesso!")