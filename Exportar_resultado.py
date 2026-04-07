import pandas as pd
from neo4j import GraphDatabase

URI = "neo4j://127.0.0.1:7687" 
USER = "neo4j"
PASSWORD = "123senha"

def exportar_consultas():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    with driver.session() as session:
        # --- CONSULTA 1: COAUTORIA (REDE) ---
        print("📊 Exportando rede de coautoria...")
        query_coautoria = """
        MATCH (a1:Autor)-[:ESCREVEU]->(art:Artigo)<-[:ESCREVEU]-(a2:Autor)
        WHERE id(a1) < id(a2)
        RETURN a1.nome AS Autor_1, a2.nome AS Autor_2, count(art) AS Trabalhos_Juntos
        ORDER BY Trabalhos_Juntos DESC
        """
        result_coautoria = session.run(query_coautoria)
        df_coautoria = pd.DataFrame([dict(record) for record in result_coautoria])
        df_coautoria.to_csv("rede_coautoria.csv", index=False, encoding="utf-8-sig")

        # --- CONSULTA 2: RANKING INSTITUIÇÕES ---
        print("🏢 Exportando ranking de instituições...")
        query_inst = """
        MATCH (i:Instituicao)<-[:AFILIADO_A]-(a:Autor)-[:ESCREVEU]->(art:Artigo)
        RETURN i.nome AS Instituicao, count(DISTINCT art) AS Total_Artigos
        ORDER BY Total_Artigos DESC
        """
        result_inst = session.run(query_inst)
        df_inst = pd.DataFrame([dict(record) for record in result_inst])
        df_inst.to_csv("ranking_instituicoes.csv", index=False, encoding="utf-8-sig")

    driver.close()
    print("\n✅ Arquivos 'rede_coautoria.csv' e 'ranking_instituicoes.csv' gerados com sucesso!")

if __name__ == "__main__":
    exportar_consultas()