import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase

# --- CONFIGURAÇÕES ---
URI = "neo4j+s://17d5c27a.databases.neo4j.io"
USER = "17d5c27a"
PASSWORD = "cnBdJkxHF7GXaSkwnqFfxihdOFqwvBvb0GgSXFfxfng"

URL_ARTIGO = "https://journals-sol.sbc.org.br/index.php/jidm/article/view/3383"

def extrair_dados_reais(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 1. Título
    titulo_tag = soup.find("h1", class_="page_title")
    titulo = titulo_tag.get_text(strip=True) if titulo_tag else "Sem Título"

    # 2. Data de Publicação
    data_div = soup.find("div", class_="item published") or soup.find("section", class_="item published")
    data_pub = "Data Desconhecida"
    if data_div:
        val = data_div.find("span", class_="value") or data_div.find("span")
        if val: data_pub = val.get_text(strip=True)

    # 3. Volume e Edição (Captura o texto exato da revista)
    volume_tag = soup.find("section", class_="item issue") or soup.find("div", class_="item issue")
    volume_info = "Volume Indefinido"
    if volume_tag:
        link_vol = volume_tag.find("a", class_="title")
        if link_vol:
            volume_info = link_vol.get_text(strip=True)

    # 4. Autores e Instituições
    autores_lista = []
    autores_tags = soup.select('ul.authors_list > li') or soup.select('.authors li')
    for li in autores_tags:
        nome_tag = li.find('span', class_='name') or li.find('strong')
        if nome_tag:
            nome = nome_tag.get_text(strip=True)
            inst_tag = li.find('span', class_='affiliation')
            inst = inst_tag.get_text(strip=True) if inst_tag else "Instituição não informada"
            autores_lista.append({"nome": nome, "inst": inst})

    # 5. DOI
    doi_div = soup.find("div", class_="item doi") or soup.find("section", class_="item doi")
    doi = "Sem DOI"
    if doi_div:
        doi_link = doi_div.find("a")
        if doi_link:
            doi = doi_link.get_text(strip=True).replace("https://doi.org/", "")

    # 6. Keywords
    kw_div = soup.find("section", class_="item keywords") or soup.find("div", class_="item keywords")
    keywords = []
    if kw_div:
        kw_val = kw_div.find("span", class_="value")
        if kw_val:
            texto_kw = kw_val.get_text(strip=True)
            keywords = [k.strip().capitalize() for k in texto_kw.replace('.', '').split(',')]

    return {
        "titulo": titulo, "data": data_pub, "doi": doi, "volume": volume_info,
        "autores": autores_lista, "keywords": keywords, "url": url
    }

def salvar_no_neo4j(dados):
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:
        # Criar Nó Artigo
        session.run("MERGE (a:Artigo {titulo: $titulo}) SET a.url = $url", titulo=dados['titulo'], url=dados['url'])

        # Criar Nó Data
        session.run("""
            MATCH (a:Artigo {titulo: $titulo})
            MERGE (d:Data {valor: $data})
            MERGE (a)-[:PUBLICADO_EM]->(d)
        """, titulo=dados['titulo'], data=dados['data'])

        # NOVO: Criar Nó Volume/Edição
        session.run("""
            MATCH (a:Artigo {titulo: $titulo})
            MERGE (v:Volume {nome: $vol})
            MERGE (a)-[:PERTENCE_AO_VOLUME]->(v)
        """, titulo=dados['titulo'], vol=dados['volume'])

        # Criar Nó DOI
        if dados['doi'] != "Sem DOI":
            session.run("""
                MATCH (a:Artigo {titulo: $titulo})
                MERGE (id:DOI {codigo: $doi})
                MERGE (a)-[:IDENTIFICADO_POR]->(id)
            """, titulo=dados['titulo'], doi=dados['doi'])

        # Criar Autores e Instituições
        for autor in dados['autores']:
            session.run("""
                MATCH (a:Artigo {titulo: $titulo})
                MERGE (au:Autor {nome: $nome})
                MERGE (i:Instituicao {nome: $inst})
                MERGE (au)-[:ESCREVEU]->(a)
                MERGE (au)-[:AFILIADO_A]->(i)
            """, titulo=dados['titulo'], nome=autor['nome'], inst=autor['inst'])

        # Criar Keywords
        for kw in dados['keywords']:
            session.run("""
                MATCH (a:Artigo {titulo: $titulo})
                MERGE (k:PalavraChave {nome: $kw})
                MERGE (a)-[:TEM_ASSUNTO]->(k)
            """, titulo=dados['titulo'], kw=kw)

    driver.close()
    print(f"Grafo reconstruído para: Analysis of Expenses from Brazilian Federal Deputies between 2015 and 2018: {dados['titulo']}")

if __name__ == "__main__":
    info = extrair_dados_reais(URL_ARTIGO)
    salvar_no_neo4j(info)