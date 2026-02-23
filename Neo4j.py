from neo4j import GraphDatabase

URI = "neo4j+s://17d5c27a.databases.neo4j.io"
USER = "17d5c27a"
PASSWORD = "cnBdJkxHF7GXaSkwnqFfxihdOFqwvBvb0GgSXFfxfng"

def conectar():
    try:
        # Criando o driver
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        
        with driver.session() as session:
            resultado = session.run("RETURN 'Conexão estabelecida com sucesso!' AS msg").single()
            print(f"✅ {resultado['msg']}")
            
            session.run("MERGE (a:Artigo {titulo: 'Inicio do Projeto', status: 'Online'})")
            print("📝 Nó de teste criado no banco 'Artigos_Info'.")
            
        driver.close()
    except Exception as e:
        print(f"ERRO: {e}")

if __name__ == "__main__":
    conectar()