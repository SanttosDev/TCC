# import requests
# from bs4 import BeautifulSoup
# from neo4j import GraphDatabase
# import time

# # --- CONFIGURAÇÕES DO NEO4J ---
# URI = "neo4j://127.0.0.1:7687" 
# USER = "neo4j"
# PASSWORD = "123senha"

# # --- DICIONÁRIOS DE TRATAMENTO (DATA CLEANING) ---

# MAPA_INSTITUICOES = {
#     "University of São Paulo": "USP",
#     "Universidade de São Paulo": "USP",
#     "ICMC-USP": "USP",
#     "ICMC - USP": "USP",
#     "ICMC - USP at São Carlos": "USP",
#     "University of São Paulo (ICMC/USP)": "USP",
#     "ICMC-USP, Sao Carlos": "USP",
#     "University of São Paulo, USP": "USP",
#     "Universidade Federal de Minas Gerais": "UFMG",
#     "UFMG": "UFMG",
#     "Universidade Federal Fluminense": "UFF",
#     "UFF": "UFF",
#     "Fluminense Federal University": "UFF",
#     "Institute of Computing - UFF": "UFF",
#     "UFRJ": "UFRJ",
#     "COPPE/UFRJ": "UFRJ",
#     "No affiliation declared": "N/A",
#     "Instituição não informada": "N/A"
# }

# MAPA_AUTORES = {
#     "Wellington S. Martins": "Wellington Santos Martins",
#     "Willian D Oliveira": "Willian Dener de Oliveira",
#     "Willian D. Oliveira": "Willian Dener de Oliveira",
#     "Yenier T. Izquierdo": "Yenier Torres Izquierdo",
#     "Zenilton K.G. Patrocínio Jr.": "Zenilton K. G. Patrocínio Jr.",
#     "Ângelo Brayner": "Angelo Brayner"
# }

# def tratar_dado(valor, mapa):
#     """Retorna o valor mapeado ou o original caso não encontre no dicionário."""
#     valor_limpo = valor.strip()
#     return mapa.get(valor_limpo, valor_limpo)

# def extrair_dados(url):
#     headers = {'User-Agent': 'Mozilla/5.0'}
#     try:
#         response = requests.get(url, headers=headers, timeout=15)
#         soup = BeautifulSoup(response.text, 'html.parser')

#         titulo_tag = soup.find("h1", class_="page_title")
#         if not titulo_tag: return None
#         titulo = titulo_tag.get_text(strip=True)

#         data_div = soup.find("div", class_="item published") or soup.find("section", class_="item published")
#         data_pub = data_div.find("span", class_="value").get_text(strip=True) if data_div and data_div.find("span", class_="value") else "Data Desconhecida"

#         # Tratamento imediato para Data
#         if "Data Desconhecida" in data_pub:
#             data_pub = "Indefinida"

#         volume_tag = soup.find("section", class_="item issue") or soup.find("div", class_="item issue")
#         volume_info = volume_tag.find("a", class_="title").get_text(strip=True) if volume_tag and volume_tag.find("a", class_="title") else "Volume Indefinido"

#         autores_lista = []
#         autores_tags = soup.select('ul.authors_list > li') or soup.select('.authors li')
#         for li in autores_tags:
#             nome_tag = li.find('span', class_='name') or li.find('strong')
#             if nome_tag:
#                 nome_bruto = nome_tag.get_text(strip=True)
#                 inst_tag = li.find('span', class_='affiliation')
#                 inst_bruta = inst_tag.get_text(strip=True) if inst_tag else "Instituição não informada"
                
#                 # APLICAÇÃO DOS TRATAMENTOS
#                 autores_lista.append({
#                     "nome": tratar_dado(nome_bruto, MAPA_AUTORES),
#                     "inst": tratar_dado(inst_bruta, MAPA_INSTITUICOES)
#                 })

#         doi_div = soup.find("div", class_="item doi") or soup.find("section", class_="item doi")
#         doi = doi_div.find("a").get_text(strip=True).replace("https://doi.org/", "") if doi_div and doi_div.find("a") else "Sem DOI"

#         kw_div = soup.find("section", class_="item keywords") or soup.find("div", class_="item keywords")
#         keywords = []
#         if kw_div:
#             kw_val = kw_div.find("span", class_="value")
#             if kw_val:
#                 keywords = [k.strip().capitalize() for k in kw_val.get_text(strip=True).replace('.', '').split(',')]

#         return {
#             "titulo": titulo, "data": data_pub, "doi": doi, "volume": volume_info,
#             "autores": autores_lista, "keywords": keywords, "url": url
#         }
#     except Exception as e:
#         print(f"⚠️ Erro ao acessar {url}: {e}")
#         return None

# def salvar_no_neo4j(tx, dados):
#     # O MERGE agora funcionará perfeitamente pois os nomes já chegam padronizados
#     tx.run("MERGE (a:Artigo {titulo: $titulo}) SET a.url = $url", titulo=dados['titulo'], url=dados['url'])
#     tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (d:Data {valor: $data}) MERGE (a)-[:PUBLICADO_EM]->(d)", 
#            titulo=dados['titulo'], data=dados['data'])
#     tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (v:Volume {nome: $vol}) MERGE (a)-[:PERTENCE_AO_VOLUME]->(v)", 
#            titulo=dados['titulo'], vol=dados['volume'])

#     if dados['doi'] != "Sem DOI":
#         tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (id:DOI {codigo: $doi}) MERGE (a)-[:IDENTIFICADO_POR]->(id)", 
#                titulo=dados['titulo'], doi=dados['doi'])

#     for autor in dados['autores']:
#         tx.run("""
#             MATCH (a:Artigo {titulo: $titulo})
#             MERGE (au:Autor {nome: $nome})
#             MERGE (i:Instituicao {nome: $inst})
#             MERGE (au)-[:ESCREVEU]->(a)
#             MERGE (au)-[:AFILIADO_A]->(i)
#         """, titulo=dados['titulo'], nome=autor['nome'], inst=autor['inst'])

#     for kw in dados['keywords']:
#         tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (k:PalavraChave {nome: $kw}) MERGE (a)-[:TEM_ASSUNTO]->(k)", 
#                titulo=dados['titulo'], kw=kw)

# if __name__ == "__main__":
#     with open("links_artigos.txt", "r") as f:
#         urls_para_processar = [l.strip() for l in f.readlines() if l.startswith("http")]
    
#     print(f"📂 Encontrados {len(urls_para_processar)} links. Iniciando carga tratada...")
    
#     driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
#     with driver.session() as session:
#         for i, url in enumerate(urls_para_processar):
#             print(f"[{i+1}/{len(urls_para_processar)}] 🔍 Processando: {url}")
#             dados = extrair_dados(url)
#             if dados:
#                 session.execute_write(salvar_no_neo4j, dados)
#                 print(f"✅ Sucesso: {dados['titulo'][:50]}...")
#             time.sleep(1)

#     driver.close()
#     print("\n🚀 Grafo populado e TRATADO com sucesso!")

import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import time
import re

# --- CONFIGURAÇÕES DO NEO4J ---
URI = "neo4j://127.0.0.1:7687" 
USER = "neo4j"
PASSWORD = "123senha"

def normalizar_autor(nome):
    if not nome: return "Autor Desconhecido"
    
    # 1. Limpeza básica de strings
    n = nome.strip()
    n = n.replace(".", ". ") # Garante espaço após pontos: "A.B." -> "A. B."
    n = re.sub(r'\s+', ' ', n) # Remove espaços duplos
    n = n.replace("Jr.", "Jr").replace("Jr", "Jr.") # Padroniza Jr.
    
    # 2. SEU DICIONÁRIO DE VERSÕES LONGAS (O "Cérebro" do tratamento)
    regras_especificas = {
        "Agma Traina": "Agma J. M. Traina",
        "Agma J M Traina": "Agma J. M. Traina",
        "Agma Juci Machado Traina": "Agma J. M. Traina",
        "Caetano Traina Jr": "Caetano Traina Jr.",
        "Altigran S da Silva": "Altigran Soares da Silva",
        "Altigran S. da Silva": "Altigran Soares da Silva",
        "Clodoveu Augusto Davis Jr": "Clodoveu A. Davis Jr.",
        "Clodoveu Augusto Davis Jr.": "Clodoveu A. Davis Jr.",
        "Cristina Ciferri": "Cristina Dutra de Aguiar Ciferri",
        "Cristina Dutra Aguiar Ciferri": "Cristina Dutra de Aguiar Ciferri",
        "Ricardo Ciferri": "Ricardo Rodrigues Ciferri",
        "Marta Mattoso": "Marta Lima Queiros Mattoso",
        "Wellington S. Martins": "Wellington Santos Martins",
        "Willian D. Oliveira": "Willian Dener de Oliveira",
        "Angelo Brayner": "Ângelo Brayner"
    }
    
    for curto, longo in regras_especificas.items():
        if curto in n:
            return longo
            
    return n

def normalizar_instituicao(nome):
    if not nome or any(x in nome.lower() for x in ["não informada", "no affiliation", "n/a", "none"]):
        return "N/A"
    
    n = nome.upper()
    
    # Lógica de Palavras-Chave (A "Rede" que captura variações)
    if any(x in n for x in ["SÃO PAULO", "USP", "ICMC", "EESC"]):
        if "FEDERAL" in n and "CARLOS" in n: return "UFSCar"
        return "USP"
    if any(x in n for x in ["FLUMINENSE", "UFF"]):
        return "UFF"
    if any(x in n for x in ["MINAS GERAIS", "UFMG"]):
        return "UFMG"
    if any(x in n for x in ["RIO DE JANEIRO", "UFRJ", "COPPE", "PPGI"]):
        if "RURAL" in n: return "UFRRJ"
        if "ESTADUAL" in n: return "UERJ"
        return "UFRJ"
    if "CAMPINAS" in n or "UNICAMP" in n:
        return "UNICAMP"
    if "PERNAMBUCO" in n or "UFPE" in n:
        return "UFPE"
    if "CEARÁ" in n or "CEARA" in n or "UFC" in n:
        if "UFCG" in n or "CAMPINA GRANDE" in n: return "UFCG"
        return "UFC"
    if "RIO GRANDE DO SUL" in n or "UFRGS" in n:
        return "UFRGS"
    if "SANTA CATARINA" in n or "UFSC" in n:
        return "UFSC"
    if "AMAZONAS" in n or "UFAM" in n:
        return "UFAM"
    if "PARANÁ" in n or "PARANA" in n or "UFPR" in n:
        return "UFPR"
    if "BRASÍLIA" in n or "BRASILIA" in n or "UNB" in n:
        return "UnB"
    if "PUC" in n or "PONTIFÍCIA" in n or "PONTIFICIA" in n:
        if "RIO" in n: return "PUC-Rio"
        if "MINAS" in n: return "PUC-Minas"
        if "PR" in n: return "PUC-PR"
        return "PUC"

    return nome.strip()

# --- FUNÇÕES DE CARGA ---

def extrair_dados(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        titulo_tag = soup.find("h1", class_="page_title")
        if not titulo_tag: return None
        titulo = titulo_tag.get_text(strip=True)

        autores_lista = []
        autores_tags = soup.select('ul.authors_list > li')
        for li in autores_tags:
            nome_tag = li.find('span', class_='name')
            if nome_tag:
                inst_tag = li.find('span', class_='affiliation')
                inst_raw = inst_tag.get_text(strip=True) if inst_tag else ""
                
                # APLICAÇÃO DOS DOIS TRATAMENTOS
                autores_lista.append({
                    "nome": normalizar_autor(nome_tag.get_text(strip=True)),
                    "inst": normalizar_instituicao(inst_raw)
                })

        data_tag = soup.select_one(".item.published .value")
        data_pub = data_tag.get_text(strip=True) if data_tag else "Indefinida"
        
        kw_tag = soup.select_one(".item.keywords .value")
        keywords = [k.strip().capitalize() for k in kw_tag.get_text(strip=True).replace('.', '').split(',')] if kw_tag else []

        return {
            "titulo": titulo, "data": data_pub, "autores": autores_lista, 
            "keywords": keywords, "url": url
        }
    except Exception as e:
        print(f"⚠️ Erro em {url}: {e}")
        return None

def salvar_no_neo4j(tx, dados):
    # O MERGE unifica os nós se o nome for igual após o tratamento
    tx.run("MERGE (a:Artigo {titulo: $titulo}) SET a.url = $url", titulo=dados['titulo'], url=dados['url'])
    tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (d:Data {valor: $data}) MERGE (a)-[:PUBLICADO_EM]->(d)", 
           titulo=dados['titulo'], data=dados['data'])
    
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
    with open("links_artigos.txt", "r") as f:
        urls = [l.strip() for l in f.readlines() if l.startswith("http")]
    
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:
        # IMPORTANTE: Limpar o banco antes de rodar a carga tratada
        print("🧹 Limpando dados antigos para garantir a integridade...")
        session.run("MATCH (n) DETACH DELETE n")
        
        print(f"🚀 Iniciando carga tratada de {len(urls)} artigos...")
        for i, url in enumerate(urls):
            dados = extrair_dados(url)
            if dados:
                session.execute_write(salvar_no_neo4j, dados)
                print(f"[{i+1}/{len(urls)}] ✅ {dados['titulo'][:40]}...")
    driver.close()
    print("\n🏁 Grafo populado com sucesso e dados UNIFICADOS!")