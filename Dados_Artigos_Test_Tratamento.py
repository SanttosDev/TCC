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
    if not texto: return ""
    texto = texto.upper()
    # Remove acentos
    texto = ''.join(c for c in unicodedata.normalize('NFKD', texto) if unicodedata.category(c) != 'Mn')
    # Mantém apenas letras e números (limpa pontos, vírgulas e parênteses)
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    return ' '.join(texto.split())

def normalizar_instituicao(nome_bruto):
    nome = pre_processar(nome_bruto)
    
    if not nome or any(x in nome for x in ["NAO INFORMADA", "NO AFFILIATION", "DECLARED"]):
        return "Instituição Não Informada"

    # --- MAPEAMENTO AGRESSIVO ---
    # Se houver qualquer um desses termos, vira UFF
    if any(radical in nome for radical in ["UFF", "FLUMIN", "INFES"]):
        return "Universidade Federal Fluminense (UFF)"
    
    if any(radical in nome for radical in ["USP", "SAO PAULO", "ICMC", "POLITECNICA"]):
        return "Universidade de São Paulo (USP)"
    
    if any(radical in nome for radical in ["UFMG", "MINAS GERAIS"]):
        return "Universidade Federal de Minas Gerais (UFMG)"
    
    if any(radical in nome for radical in ["UFRJ", "RIO DE JANEIRO", "COPPE"]):
        return "Universidade Federal do Rio de Janeiro (UFRJ)"

    return nome

def extrair_dados(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        titulo_tag = soup.find("h1", class_="page_title")
        if not titulo_tag: return None
        titulo = titulo_tag.get_text(strip=True)

        autores_lista = []
        autores_tags = soup.select('ul.authors_list > li') or soup.select('.authors li')
        
        for li in autores_tags:
            nome_tag = li.find('span', class_='name') or li.find('strong')
            if nome_tag:
                nome = nome_tag.get_text(strip=True)
                inst_tag = li.find('span', class_='affiliation')
                inst_original = inst_tag.get_text(strip=True) if inst_tag else "N/A"
                
                # NORMALIZAÇÃO
                inst_corrigida = normalizar_instituicao(inst_original)
                
                # --- O SEU PRINT DE RASTREIO ---
                print(f">>> TESTE: {inst_original[:40]}... | FICOU: {inst_corrigida}")
                
                autores_lista.append({"nome": nome, "inst": inst_corrigida})

        return {"titulo": titulo, "url": url, "autores": autores_lista}
    except Exception as e:
        print(f"⚠️ Erro no scraping {url}: {e}")
        return None

def salvar_no_neo4j(tx, dados):
    # USAMOS A URL COMO CHAVE ÚNICA (Evita que títulos iguais 'sumam' com os dados)
    tx.run("MERGE (a:Artigo {url: $url}) SET a.titulo = $titulo", url=dados['url'], titulo=dados['titulo'])
    
    for autor in dados['autores']:
        tx.run("""
            MATCH (a:Artigo {url: $url})
            MERGE (au:Autor {nome: $nome})
            MERGE (i:Instituicao {nome: $inst})
            MERGE (au)-[:ESCREVEU]->(a)
            MERGE (au)-[:AFILIADO_A]->(i)
        """, url=dados['url'], nome=autor['nome'], inst=autor['inst'])

if __name__ == "__main__":
    # Lembre-se de rodar: MATCH (n) DETACH DELETE n antes!
    try:
        with open("links_artigos.txt", "r") as f:
            urls = [l.strip() for l in f.readlines() if l.startswith("http")]
    except:
        print("Arquivo de links não encontrado."); exit()
    
    print(f"🚀 Processando {len(urls)} links...")
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:
        for i, url in enumerate(urls):
            dados = extrair_dados(url)
            if dados:
                session.execute_write(salvar_no_neo4j, dados)
    driver.close()