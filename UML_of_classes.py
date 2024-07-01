from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import List
import pandas as pd
import csv
from processQueryData import ProcessDataQueryHandler
from UML_dataModel import Acquisition, Processing, Modelling, Optimising, Exporting




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
                        my_graph.add((URIRef(base_url +"culturalheritageobject-" + object_id), relAuthor, author_uri))  # MODIFICA!!! mancava culturalheritageobject come parte del predicato

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
        self.processQuery = []

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadata_query_handlers.append(handler)
        return True

    def addProcessHandler(self, processHandler:ProcessDataQueryHandler) -> bool:
        if not isinstance(Handler, ProcessDataQueryHandler):
            return False
        else:
            return self.processQuery.append(processHandler)  # Adds a process handler to the list
    
    
    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]:      #ritorna un oggetto della classe IdentifiableEntity identificando l'entità corrispondente all'identificatore dato nelle basi dati accessibili tramite i gestori di query; se non viene trovata nessuna entità con l'identificatore dato, ritorna None, assicurando che l'oggetto restituito appartenga alla classe appropriata.        
        for handler in self.metadata_query_handlers:
            result = handler.getById(id)

            if not result.empty:
                entity_type = result.iloc[0].get('type')
                entity_id = result.iloc[0].get('id')
                entity_title = result.iloc[0].get('title')
                entity_owner = result.iloc[0].get('owner')
                entity_place = result.iloc[0].get('place')
                entity_date = result.iloc[0].get('date', None)
                entity_authors = result.iloc[0].get('authors', [])


                if entity_type == 'Person':
                    entity = Person(id=entity_id, name=entity_title)
                elif entity_type == 'NauticalChart':
                    entity = NauticalChart(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'ManuscriptPlate':
                    entity = ManuscriptPlate(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'ManuscriptVolume':
                    entity = ManuscriptVolume(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'PrintedVolume':
                    entity = PrintedVolume(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'PrintedMaterial':
                    entity = PrintedMaterial(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Herbarium':
                    entity = Herbarium(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Specimen':
                    entity = Specimen(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Painting':
                    entity = Painting(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Model':
                    entity = Model(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                elif entity_type == 'Map':
                    entity = Map(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
                else:
                    entity = CulturalHeritageObject(id=entity_id, title=entity_title, owner=entity_owner, place=entity_place, date=entity_date, authors=entity_authors)
            

                return entity

        
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
    


    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:     #restituisce una lista di oggetti della classe CulturalHeritageObject che comprende tutte le entità incluse nel database accessibili tramite i gestori di query, garantendo che gli oggetti nella lista siano della classe appropriata,        
        all_objects = []

        for handler in self.metadata_query_handlers:
            results = handler.getAllCulturalHeritageObjects()

            for _, row in results.iterrows():
                obj_type = row['type']

                
                object_constructors = {
                    'Map': Map,
                    'Painting': Painting,
                    'Model': Model,
                    'Herbarium': Herbarium,
                    'Specimen': Specimen,
                    'ManuscriptPlate': ManuscriptPlate,
                    'ManuscriptVolume': ManuscriptVolume,
                    'PrintedVolume': PrintedVolume,
                    'PrintedMaterial': PrintedMaterial,
                }

                
                obj_constructor = object_constructors.get(obj_type, CulturalHeritageObject)

                
                obj = obj_constructor(
                    id=row['id'],
                    title=row['title'],
                    owner=row['owner'],
                    place=row['place'],
                    date=row['date'],
                    authors=row.get('authors', [])
                )

                all_objects.append(obj)
        
        return all_objects
    

#elena
def getAllActivities(self):
    result = []
    handler_list = self.processQuery  # Gets the list of process handlers
    df_list = []  # List to store the DataFrames of activities

    # Loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getAllActivities())  # Adds the activities of the handler to the list of DataFrames

    # Concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    # Loop over each row of the concatenated DataFrame
    for _, row in df_union.iterrows():
        # Splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        # Common parameters for all activity objects -- matching data model e existing data from queries according to the method getallactivities
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        # Checks the type of activity and creates the corresponding object
        if activity_type == "acquisition":
            object = Acquisition(technique=row['technique'], **common_params)
            result.append(object)

        elif activity_type == "processing":
            object = Processing(**common_params)
            result.append(object)

        elif activity_type == "modelling":
            object = Modelling(**common_params)
            result.append(object)

        elif activity_type == "optimising":
            object = Optimising(**common_params)
            result.append(object)

        elif activity_type == "exporting":
            object = Exporting(**common_params)
            result.append(object)

    return result  # Returns the list of activity objects



def getActivitiesByResponsibleInstitution(self, partialName):       
    result = []
    handler_list = self.processQuery  # Gets the list of process handlers
    df_list = []  # List to store the DataFrames of activities

    # Loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivitiesByResponsibleInstitution(partialName))  # Adds the activities of the handler to the list of DataFrames

    # Concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    # Loop over each row of the concatenated DataFrame
    for _, row in df_union.iterrows():
        # Splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        # Common parameters for all activity objects -- matching data model e existing data from queries according to the method getActivitiesByResponsibleInstitution
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        # Checks the type of activity and creates the corresponding object
        if activity_type == "acquisition":
            object = Acquisition(technique=row['technique'], **common_params)
            result.append(object)

        elif activity_type == "processing":
            object = Processing(**common_params)
            result.append(object)

        elif activity_type == "modelling":
            object = Modelling(**common_params)
            result.append(object)

        elif activity_type == "optimising":
            object = Optimising(**common_params)
            result.append(object)

        elif activity_type == "exporting":
            object = Exporting(**common_params)
            result.append(object)

    return result  # Returns the list of activity objects


def getgetActivitiesByResponsiblePerson(self, partialName):       
    result = []
    handler_list = self.processQuery  # Gets the list of process handlers
    df_list = []  # List to store the DataFrames of activities

    # Loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivitiesByResponsiblePerson(partialName))  # Adds the activities of the handler to the list of DataFrames

    # Concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    # Loop over each row of the concatenated DataFrame
    for _, row in df_union.iterrows():
        # Splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        # Common parameters for all activity objects -- matching data model e existing data from queries according to the method getgetActivitiesByResponsiblePerson
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        # Checks the type of activity and creates the corresponding object
        if activity_type == "acquisition":
            object = Acquisition(technique=row['technique'], **common_params)
            result.append(object)

        elif activity_type == "processing":
            object = Processing(**common_params)
            result.append(object)

        elif activity_type == "modelling":
            object = Modelling(**common_params)
            result.append(object)

        elif activity_type == "optimising":
            object = Optimising(**common_params)
            result.append(object)

        elif activity_type == "exporting":
            object = Exporting(**common_params)
            result.append(object)

    return result  # Returns the list of activity objects



def getActivitiesUsingTool(self, partialName: str):          
    result = []
    handler_list = self.processQuery  # Gets the list of process handlers
    df_list = []  # List to store the DataFrames of activities

    # Loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivitiesUsingTool(partialName))  # Adds the activities of the handler to the list of DataFrames

    # Concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    # Loop over each row of the concatenated DataFrame
    for _, row in df_union.iterrows():
        # Splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        # Common parameters for all activity objects -- matching data model e existing data from queries according to the method getActivitiesUsingTool
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        # Checks the type of activity and creates the corresponding object
        if activity_type == "acquisition":
            object = Acquisition(technique=row['technique'], **common_params)
            result.append(object)

        elif activity_type == "processing":
            object = Processing(**common_params)
            result.append(object)

        elif activity_type == "modelling":
            object = Modelling(**common_params)
            result.append(object)

        elif activity_type == "optimising":
            object = Optimising(**common_params)
            result.append(object)

        elif activity_type == "exporting":
            object = Exporting(**common_params)
            result.append(object)

    return result

