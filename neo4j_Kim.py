import pandas as pd
from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
  "neo4j://44.202.12.248:7687",
  auth=basic_auth("neo4j", "lead-benefits-asterisks"))

def name_query(name, output_name):
  cypher_query = '''
  MATCH (a:Officer {name:$name})-[r:officer_of|intermediary_of|registered_address*..10]-(b)
  RETURN b.name as name, b.node_id as node_id LIMIT 20
  '''
  with driver.session(database="neo4j") as session:
    results = session.execute_read(lambda tx: tx.run(cypher_query, name=name).data())
    output_name = pd.DataFrame().from_records(results)
    driver.close()
  return output_name

def city_query(city, output_name):
  cypher_query = f'''
    MATCH (b)
    WHERE toLower(b.name) CONTAINS {city}
    RETURN  *
    '''
  with driver.session(database="neo4j") as session:
    results = session.execute_read(lambda tx: tx.run(cypher_query).data())
    output_name = results[0]
    driver.close()
  return output_name

query_list_names = [] #hier Liste an Namensanfragen einfügen

for i in query_list_names:
  name = str(i)
  output_name = str(i)+'_df'
  name_query(name, output_name)


query_list_cities = [] #hier Liste an Stadtanfragen einfügen

for i in query_list_cities:
  city = str(i)
  output_name = str(i)+'_df'
  city_query(city, output_name)