from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
  "neo4j://44.202.12.248:7687",
  auth=basic_auth("neo4j", "lead-benefits-asterisks"))


def name_query(name):
    cypher_query = '''
    MATCH (a:Officer {name:$name})-[r:officer_of|intermediary_of|registered_address*..10]-(b)
    RETURN b.name as name, b.node_id as node_id LIMIT 20
    '''
    with driver.session(database="neo4j") as session:
        results = session.execute_read(lambda tx: tx.run(cypher_query, name=name).data())
        print(f'{name}: {len(results)} results')
        driver.close()


def city_query(city):
    cypher_query = f'''
    MATCH (b)
    WHERE toLower(b.name) CONTAINS toLower(" {city} ")
    RETURN  *
    '''
    with driver.session(database="neo4j") as session:
        results = session.execute_read(lambda tx: tx.run(cypher_query).data())
        print(f'{city}: {len(results)} results')
        driver.close()


query_list_names = [] #hier Liste an Namensanfragen einfügen
for name in query_list_names:
    name_query(name)

query_list_cities = [] #hier Liste an Stadtanfragen einfügen
for city in query_list_cities:
    city_query(city)