import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase

# --- CONFIGURAÇÕES ---
URI = "neo4j+s://17d5c27a.databases.neo4j.io"
USER = "17d5c27a"
PASSWORD = "cnBdJkxHF7GXaSkwnqFfxihdOFqwvBvb0GgSXFfxfng"

# Link do artigo que você quer extrair agora
URL_ARTIGO = "https://journals-sol.sbc.org.br/index.php/jidm/article/view/3383"

def processar_artigo(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 1. Extrair Título
    titulo = soup.find("h1", class_="page_title").text.strip()
    
    # 2. Extrair Autores e Instituições
    lista_autores = []
    autores_tags = soup.select('ul.authors_list > li')
    for li in autores_tags:
        nome = li.find('span', class_='name').text.strip()
        inst_tag = li.find('span', class_='affiliation')
        inst = inst_tag.text.strip() if inst_tag else "Instituição não informada"
        lista_autores.append({"nome": nome, "instituicao": inst})
        
    # 3. Extrair Palavras-chave
    keywords = []
    kw_section = soup.find("section", class_="item keywords")
    if kw_section:
        kw_text = kw_section.find("span", class_="value").text.strip()
        keywords = [k.strip().capitalize() for k in kw_text.split(',')]

    # 4. Inserir no Neo4j
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as session:
        # Query Cypher poderosa que cria tudo e evita duplicados (MERGE)
        cypher = """
        MERGE (art:Artigo {titulo: $titulo})
        SET art.url = $url
        
        FOREACH (autor_data IN $autores |
            MERGE (aut:Autor {nome: autor_data.nome})
            MERGE (inst:Instituicao {nome: autor_data.instituicao})
            MERGE (aut)-[:AFILIADO_A]->(inst)
            MERGE (aut)-[:ESCREVEU]->(art)
        )
        
        FOREACH (kw IN $keywords |
            MERGE (p:PalavraChave {nome: kw})
            MERGE (art)-[:TEM_ASSUNTO]->(p)
        )
        """
        session.run(cypher, titulo=titulo, url=url, autores=lista_autores, keywords=keywords)
    
    driver.close()
    print(f"✅ Sucesso! Artigo '{titulo}' inserido no Neo4j.")

if __name__ == "__main__":
    processar_artigo(URL_ARTIGO)