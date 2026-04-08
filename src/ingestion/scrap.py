import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import time
import pandas as pd 
import json
import os
import requests
from bs4 import BeautifulSoup

URI = "neo4j://127.0.0.1:7687"
USER = "neo4j"
PASSWORD = "senha111"

def get_neo4j_driver():
    return GraphDatabase.driver(URI, auth=(USER, PASSWORD))

try:
    driver = get_neo4j_driver()
    driver.verify_connectivity()
    print("✅ Conexão com Neo4j estabelecida!")
except Exception as e:
    print(f"❌ Erro na conexão: {e}")


pasta_destino = 'data/raw'
arquivo_links = 'links_artigos.txt'
arquivo_saida = os.path.join(pasta_destino, 'dados_artigos.json')

if not os.path.exists(pasta_destino):
    os.makedirs(pasta_destino)
    print(f"📁 Pasta '{pasta_destino}' criada.")

def extrair_dados(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
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

if __name__ == "__main__":
    if not os.path.exists(arquivo_links):
        print(f"❌ Erro: O arquivo {arquivo_links} não foi encontrado!")
    else:
        with open(arquivo_links, 'r', encoding='utf-8') as f:
            urls = [linha.strip() for linha in f.readlines() if linha.strip()]

        resultados = []
        total = len(urls)

        print(f"🚀 Iniciando extração de {total} links...")

        for i, link in enumerate(urls, 1):
            print(f"[{i}/{total}] Processando: {link}")
            dados = extrair_dados(link)
            if dados:
                resultados.append(dados)

        if resultados:
            with open(arquivo_saida, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, ensure_ascii=False, indent=4)
            print(f"\n✅ Concluído! {len(resultados)} artigos salvos em: {arquivo_saida}")
        else:
            print("\n⚠️ Nenhum dado foi extraído com sucesso.")