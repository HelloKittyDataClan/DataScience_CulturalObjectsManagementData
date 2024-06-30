from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import List
import pandas as pd
import csv






class Handler:  # Chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass

class MetadataUploadHandler(UploadHandler):  # Chiara
    def __init__(self):
        self.dbPathOrUrl = ""

    def pushDataToDb(self, path) -> bool:

            my_graph = Graph()
            # Read the data from the csv file and store them into a dataframe
            venus = pd.read_csv(path,
                                keep_default_na=False,
                                dtype={
                                    "Id": "string",
                                    "Type": "string",
                                    "Title": "string",
                                    "Date": "string",
                                    "Author": "string",
                                    "Owner": "string",
                                    "Place": "string",
                                })
            # Rimuoviamo i duplicati della colonna id, mantenendo la prima istanza
            venus.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)

            # Define namespaces
            base_url = Namespace("http://github.com/HelloKittyDataClan/DSexam/")
            db = Namespace("https://dbpedia.org/property/")
            schema = Namespace("http://schema.org/")

            # Create Graph
            my_graph.bind("base_url", base_url)
            my_graph.bind("db", db)
            my_graph.bind("FOAF", FOAF)
            my_graph.bind("schema", schema)

            # Define classes about Cultural Object
            Person = URIRef(FOAF + "Person")
            NauticalChart = URIRef(base_url + "NauticalChart")
            ManuscriptPlate = URIRef(base_url + "ManuscriptPlate")
            ManuscriptVolume = URIRef(base_url + "ManuscriptVolume")
            PrintedVolume = URIRef(base_url + "PrintedVolume")
            PrintedMaterial = URIRef(base_url + "PrintedMaterial")
            Herbarium = URIRef(db + "Herbarium")
            Specimen = URIRef(base_url + "Specimen")
            Painting = URIRef(db + "Painting")
            Model = URIRef(db + "Model")
            Map = URIRef(db + "Map")

            # Attributes related to classes
            title = URIRef(schema + "title")
            date = URIRef(schema + "dateCreated")
            place = URIRef(schema + "itemLocation")
            id = URIRef(schema + "identifier")
            owner = URIRef(base_url + "owner")

            # Relation with authors among classes
            relAuthor = URIRef(schema + "author")

            # Attributes related to the class Person
            name = URIRef(FOAF + "name")  # URI di FOAF http://xmlns.com/foaf/0.1/

            # Add to the graph the Cultural Object
            for idx, row in venus.iterrows():
                loc_id = "culturalobject-" + str(idx)
                subj = URIRef(base_url + loc_id)
                if row["Id"] != "":
                    my_graph.add((subj, id, Literal(str(row["Id"]))))

                # Assign a resource class to the object
                if row["Type"] != "":
                    if row["Type"].lower() == "nautical chart":
                        my_graph.add((subj, RDF.type, NauticalChart))
                    elif row["Type"].lower() == "manuscript plate":
                        my_graph.add((subj, RDF.type, ManuscriptPlate))
                    elif row["Type"].lower() == "manuscript volume":
                        my_graph.add((subj, RDF.type, ManuscriptVolume))
                    elif row["Type"].lower() == "printed volume":
                        my_graph.add((subj, RDF.type, PrintedVolume))
                    elif row["Type"].lower() == "printed material":
                        my_graph.add((subj, RDF.type, PrintedMaterial))
                    elif row["Type"].lower() == "herbarium":
                        my_graph.add((subj, RDF.type, Herbarium))
                    elif row["Type"].lower() == "specimen":
                        my_graph.add((subj, RDF.type, Specimen))
                    elif row["Type"].lower() == "painting":
                        my_graph.add((subj, RDF.type, Painting))
                    elif row["Type"].lower() == "model":
                        my_graph.add((subj, RDF.type, Model))
                    elif row["Type"].lower() == "map":
                        my_graph.add((subj, RDF.type, Map))

                # Assign title
                if row["Title"] != "":
                    my_graph.add((subj, title, Literal(str(row["Title"]))))
                # Assign date
                if row["Date"] != "":
                    my_graph.add((subj, date, Literal(str(row["Date"]))))
                # Assign owner
                if row["Owner"] != "":
                    my_graph.add((subj, owner, Literal(str(row["Owner"]))))
                # Assign place
                if row["Place"] != "":
                    my_graph.add((subj, place, Literal(str(row["Place"]))))

            # Populating the graph with all the people
            author_id_mapping = dict()
            object_mapping = dict()
            people_counter = 0

            for idx, row in venus.iterrows():
                if row["Author"] != "":
                    author_list = row["Author"].split(";")
                    for author in author_list:
                        if "(" in author and ")" in author:
                            split_index = author.index("(")
                            author_name = author[:split_index - 1].strip()
                            author_id = author[split_index + 1:-1].strip()

                            object_id = row["Id"]

                            if author_id in author_id_mapping:
                                person_uri = author_id_mapping[author_id]
                            else:
                                local_id = "person-" + str(people_counter)
                                person_uri = URIRef(base_url + local_id)
                                my_graph.add((person_uri, RDF.type, Person))
                                my_graph.add((person_uri, id, Literal(author_id)))
                                my_graph.add((person_uri, name, Literal(author_name)))
                                author_id_mapping[author_id] = person_uri
                                people_counter += 1

                            if object_id in object_mapping:
                                object_mapping[object_id].add(person_uri)
                            else:
                                object_mapping[object_id] = {person_uri}

                # Aggiungi l'assegnazione degli autori al grafo
                for object_id, authors in object_mapping.items():
                    for author_uri in authors:
                        my_graph.add((URIRef(base_url +"culturalheritageobject-" + object_id), relAuthor, author_uri))  # MODIFICA mancava cultural object come parte del predicato

            # Store RDF data in SPARQL endpoint
            store = SPARQLUpdateStore()
            endpoint = self.getDbPathOrUrl() + "sparql"
            store.open((endpoint, endpoint))  # First parameter reading data, second write database

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()

            # Verificare se il grafo è stato caricato 
            query_graph = """
            SELECT ?s ?p ?o
            WHERE {
                ?s ?p ?o .
            }
            """
            result_dataset = get(endpoint, query_graph, True)

            num_triples_blazegraph = len(result_dataset)
            num_triples_local = len(my_graph)

            pos = num_triples_blazegraph == num_triples_local
            if pos:
                print("I dati sono stati caricati con successo su Blazegraph.")
                return True
            else:
                print("Caricamento dei dati su Blazegraph non riuscito.")
                return False


'''prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix schema: <http://schema.org/>
prefix FOAF: <http://xmlns.com/foaf/0.1/>
       

SELECT ?culturalObjectURI ?culturalObjectId
WHERE {
            ?culturalObjectURI rdf:type ?type .
            FILTER(?type != FOAF:Person)
            ?culturalObjectURI schema:identifier ?culturalObjectId .
        }'''




# java -server -Xmx1g -jar blazegraph.jar (terminal command to run Blazegraph)
#Bea
class MetadataQueryHandler(QueryHandler):
    def __init__(self, grp_dbUrl: str):
        super().__init__(dbPathOrUrl = grp_dbUrl)

     def get(self, query, parse_results=True):
        sparql = SPARQLWrapper(self.dbPathOrUrl + "sparql")        #modificata leggeremente per renderla più robusta e comprensibile 
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)

        try:
            sparql_result = sparql.queryAndConvert()
            if parse_results:
                if 'head' not in sparql_result or 'results' not in sparql_result:
                    return pd.DataFrame()

                df_columns = sparql_result["head"]["vars"]
                rows = []
                for result in sparql_result["results"]["bindings"]:
                    row_dict = {column: result[column]["value"] if column in result else None for column in df_columns}
                    rows.append(row_dict)
                df = pd.DataFrame(rows, columns=df_columns)
                return df
            else:
                return sparql_result  

        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()   


   
    
    def getById(self, id):
        person_query_str = """
            SELECT DISTINCT ?uri ?name ?id 
            WHERE {
                ?uri <http://schema.org/identifier> "%s" ;
                     <http://xmlns.com/foaf/0.1/name> ?name ;
                     <http://schema.org/identifier> ?id .
                ?object <http://schema.org/author> ?uri .
            }
        """ % id
        
        object_query_str = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {
                ?object <http://schema.org/identifier> "%s" .
                ?object <http://schema.org/identifier> ?id  .
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
                ?object <http://schema.org/title> ?title .
                ?object <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner .
                ?object <http://schema.org/itemLocation> ?place .
                 
                OPTIONAL {
                    ?object <http://schema.org/dateCreated> ?date .
                    ?object <http://schema.org/author> ?author .
                    ?author <http://xmlns.com/foaf/0.1/name> ?author_name .
                    ?author <http://schema.org/identifier> ?author_id .
                }
            }

        """ % id
        person_df = self.get(person_query_str)
        object_df = self.get(object_query_str)

        if person_df.empty and object_df.empty:
            return pd.DataFrame()  # Return an empty DataFrame if no results

        combined_df = pd.concat([person_df, object_df], ignore_index=True)
        return combined_df
    
    
    def getAllPeople(self):
        query = """
        PREFIX FOAF: <http://xmlns.com/foaf/0.1/>
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?uri ?author_name ?author_id
        WHERE {
            ?uri a FOAF:Person ;
                 schema:identifier ?author_id ;
                 FOAF:name ?author_name .
        }
        """
        results = self.get(query, parse_results=True)  # Esegui la query SPARQL

        # Costruisci il DataFrame a partire dai risultati della query
        if results.empty:
            return pd.DataFrame()  # Ritorna un DataFrame vuoto se non ci sono risultati

        # Costruisci il DataFrame utilizzando i dati ottenuti
        df = pd.DataFrame(results, columns=['uri', 'author_name', 'author_id'])  # Aggiungi altre colonne se necessario

        return df

     
    




    
    
    
    def getAllCulturalHeritageObjects(self):
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX db: <https://dbpedia.org/property/>

        SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?authorName ?authorID
        WHERE {
            ?object a ?type ;
                    schema:identifier ?id ;
                    schema:title ?title ;
                    base_url:owner ?owner ;
                    schema:itemLocation ?place .
                
            OPTIONAL { ?object schema:dateCreated ?date }
            OPTIONAL {
                ?object schema:author ?author .
                ?author foaf:name ?authorName ;
                schema:identifier ?authorID .
            }
        
            FILTER (?type IN (
                base_url:NauticalChart,
                base_url:ManuscriptPlate,
                base_url:ManuscriptVolume,
                base_url:PrintedVolume,
                base_url:PrintedMaterial,
                db:Herbarium,
                base_url:Specimen,
                db:Painting,
                db:Model,
                db:Map
            ))
        }
        """
        return self.get(query)
       
    
        



    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:        #modifica per ottenere un object id con la query giusta
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?authorName ?authorID 
        WHERE {{
          ?object schema:identifier "{object_id}" ;
                  schema:author ?uri .

          ?uri schema:identifier ?authorID ;
               foaf:name ?authorName .
        }} 
        """

        return self.get(query)


    
    def getCulturalHeritageObjectsAuthoredBy(self, personid: str) -> pd.DataFrame:
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX base_url: <http://github.com/HelloKittyDataClan/DSexam/>
        PREFIX db: <https://dbpedia.org/property/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?authorName ?authorID
        WHERE {{
            ?object a ?type ;
                    schema:identifier ?id ;
                    schema:title ?title ;
                    base_url:owner ?owner ;
                    schema:itemLocation ?place ;
                    schema:author ?author .

            ?author foaf:name ?authorName ;
                    schema:identifier ?authorID ;
                    schema:identifier "{personid}" .

            OPTIONAL {{ ?object schema:dateCreated ?date }}

            FILTER (?type IN (
                base_url:NauticalChart,
                base_url:ManuscriptPlate,
                base_url:ManuscriptVolume,
                base_url:PrintedVolume,
                base_url:PrintedMaterial,
                db:Herbarium,
                base_url:Specimen,
                db:Painting,
                db:Model,
                db:Map
            ))
        }}
        """

        return self.get(query)


#BasicMashup

class BasicMashup:
    def __init__(self):
        self.metadata_query_handlers = []
    

   

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadata_query_handlers.append(handler)
        return True

        
    
    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]:   #gentilmente da rivedere domani insieme e capire il problema    
        print(f"Searching for entity with ID: {id}")
        for handler in self.metadata_query_handlers:
            result = handler.getById(id)
            print(f"Result from handler: {result}")

            if not result.empty:
                entity_type = result.iloc[0].get('type')
                entity_name = result.iloc[0].get('name')
                print(f"Found entity type: {entity_type}, name: {entity_name}")

                if entity_type == 'Person':
                    entity = Person(id=id, name=entity_name)
                else:
                    entity = IdentifiableEntity(id=id)

                print(f"Found entity: {entity.__class__.__name__} with ID: {entity.id}")
                if isinstance(entity, Person):
                    print(f"Name: {entity.name}")
                
                return entity

        print("No entity found")
        return None



    
    
    

    def getAllPeople(self):                                            #restituisce la lista delle persone 
        # Ottieni tutte le persone usando MetadataQueryHandler
        people = []
        for handler in self.metadata_query_handlers:
            people_data = handler.getAllPeople()
            for _, person_data in people_data.iterrows():
                person = Person(id=person_data['author_id'], name=person_data['author_name'])
                people.append(person)
        return people
    


    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:                                   #restituisce un elenco di oggetti CulturaalHeritageObject dal Database, ciascuno appartenente alla classe propria
        all_objects = []
        for handler in self.metadata_query_handlers:
            results = handler.getAllCulturalHeritageObjects()
            for _, row in results.iterrows():
                if row['type'] == 'Map':
                    obj = Map(id=row['id'], title=row['title'], owner=row['owner'], place=row['place'], date=row['date'])
                else:
                    obj = CulturalHeritageObject(id=row['id'], title=row['title'], owner=row['owner'], place=row['place'], date=row['date'])
                all_objects.append(obj)
        return all_objects
