import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import time

# --- CONFIGURAÇÕES DO NEO4J ---
URI = "neo4j://127.0.0.1:7687"
USER = "neo4j"
PASSWORD = "senha111"

def extrair_dados(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Título
        titulo_tag = soup.find("h1", class_="page_title")
        if not titulo_tag: return None
        titulo = titulo_tag.get_text(strip=True)

        # Data
        data_div = soup.find("div", class_="item published") or soup.find("section", class_="item published")
        data_pub = data_div.find("span", class_="value").get_text(strip=True) if data_div and data_div.find("span", class_="value") else "Data Desconhecida"

        # Volume/Edição
        volume_tag = soup.find("section", class_="item issue") or soup.find("div", class_="item issue")
        volume_info = volume_tag.find("a", class_="title").get_text(strip=True) if volume_tag and volume_tag.find("a", class_="title") else "Volume Indefinido"

        # Autores e Instituições
        autores_lista = []
        autores_tags = soup.select('ul.authors_list > li') or soup.select('.authors li')
        for li in autores_tags:
            nome_tag = li.find('span', class_='name') or li.find('strong')
            if nome_tag:
                nome = nome_tag.get_text(strip=True)
                inst_tag = li.find('span', class_='affiliation')
                inst = inst_tag.get_text(strip=True) if inst_tag else "Instituição não informada"
                autores_lista.append({"nome": nome, "inst": inst})

        # DOI
        doi_div = soup.find("div", class_="item doi") or soup.find("section", class_="item doi")
        doi = doi_div.find("a").get_text(strip=True).replace("https://doi.org/", "") if doi_div and doi_div.find("a") else "Sem DOI"

        # Keywords
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
    # Cria o Artigo
    tx.run("MERGE (a:Artigo {titulo: $titulo}) SET a.url = $url", titulo=dados['titulo'], url=dados['url'])
    
    # Conecta Data
    tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (d:Data {valor: $data}) MERGE (a)-[:PUBLICADO_EM]->(d)", 
           titulo=dados['titulo'], data=dados['data'])
    
    # Conecta Volume
    tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (v:Volume {nome: $vol}) MERGE (a)-[:PERTENCE_AO_VOLUME]->(v)", 
           titulo=dados['titulo'], vol=dados['volume'])

    # Conecta DOI
    if dados['doi'] != "Sem DOI":
        tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (id:DOI {codigo: $doi}) MERGE (a)-[:IDENTIFICADO_POR]->(id)", 
               titulo=dados['titulo'], doi=dados['doi'])

    # Conecta Autores e Instituições
    for autor in dados['autores']:
        tx.run("""
            MATCH (a:Artigo {titulo: $titulo})
            MERGE (au:Autor {nome: $nome})
            MERGE (i:Instituicao {nome: $inst})
            MERGE (au)-[:ESCREVEU]->(a)
            MERGE (au)-[:AFILIADO_A]->(i)
        """, titulo=dados['titulo'], nome=autor['nome'], inst=autor['inst'])

    # Conecta Keywords
    for kw in dados['keywords']:
        tx.run("MATCH (a:Artigo {titulo: $titulo}) MERGE (k:PalavraChave {nome: $kw}) MERGE (a)-[:TEM_ASSUNTO]->(k)", 
               titulo=dados['titulo'], kw=kw)

if __name__ == "__main__":
    # 1. Carregar links do arquivo
    with open("links_artigos.txt", "r") as f:
        linhas = f.readlines()
    
    # Filtrar apenas o que começa com http
    urls_para_processar = [l.strip() for l in linhas if l.startswith("http")]
    
    print(f"📂 Encontrados {len(urls_para_processar)} links para processar.")
    
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    with driver.session() as session:
        for i, url in enumerate(urls_para_processar):
            print(f"[{i+1}/{len(urls_para_processar)}] 🔍 Extraindo: {url}")
            dados = extrair_dados(url)
            
            if dados:
                session.execute_write(salvar_no_neo4j, dados)
                print(f"✅ Sucesso: {dados['titulo'][:50]}...")
            
            # Pequena pausa para não sobrecarregar o site da revista
            time.sleep(1)

    driver.close()
    print("\n🚀 Grafo populado com sucesso com todos os artigos do arquivo!")