from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import List
import pandas as pd
import csv
from processQueryData import ProcessDataQueryHandler
from UML_dataModel import Acquisition, Processing, Modelling, Optimising, Exporting
from sparql_dataframe import get
from typing import Optional, List, Any


class IdentifiableEntity(object): #identifichiamo l'ID
    def __init__(self, id: str):
        if not isinstance(id, str): #se l'ID non è una stringa
            raise ValueError("ID must be a string for the IdentifiableEntity")
        self.id = id #deve essere necessariamente una stringa cosi che l'ID sia valido sempre

    def get_id(self):
        return self.id

#___________________________CSV_________________________

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id:str, title:str, owner:str, place:str, date:str= None,authors:list=None): #vado a definire title, date, owner, place, Author  del mio csv 
        super().__init__(id)  #cosi facendo vado a richiamare l'ID della classe IdentifiableEntity
        if not isinstance(title, str):
            raise ValueError("Title must be a string for the CulturalObject")
        if not isinstance(owner, str):
            raise ValueError("Owner must be a string for the CulturalObject")
        if not isinstance(place, str):
            raise ValueError("Place must be a string for the CulturalObject")
        if date is not None and not isinstance(date, str):
            raise ValueError("Date must be a string or None for the CulturalObject")
        if authors is not None:
            if not all(isinstance(author, Person) for author in authors):
                raise ValueError("Authors must be instances of Person or None for the CulturalObject")    # garantisce che gli autori forniti per il nostro oggetto culturale siano parte della classe Person
        
        self.title=title
        self.date=date
        self.owner=owner
        self.place=place
        self.authors=[]    #lista vuota assegnata all'attributo authors 

        if authors:
            if isinstance(authors, Person):
                self.authors.append(authors)
            elif isinstance(authors, list):
                self.authors.extend(authors)       #si occupa di aggiungere gli autori forniti all'oggetto culturale CulturalObject, gestendo sia il caso in cui sia fornito un singolo autore come istanza di Person, sia il caso in cui siano forniti più autori come lista di istanze di Person.
   
    def getTitle(self):
        return self.title

    def getDate(self):
        if self.date:
           return self.date
        return None
        
    def getOwner(self):
        return self.owner

    def getPlace(self):
        return self.place

    def getAuthors(self):
        return self.authors


#definiamo le sottoclassi relative alla classe Cultrual Object   
class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass

#____________________ JSON______________________

#Creation of class Person that refers to CulturalHeritageObject
class Person(IdentifiableEntity):
    def __init__(self, name: str): #define parameter name
        super().__init__(id)
        if not isinstance(name, str):
            raise ValueError("Name must be a string for the Person")
        self.name = name
    
    def getName(self):
        return self.name

#Creation of class Activity that refers to CulturalHeritageObject
class Activity():                               
    def _init_(self, institute: str, person: str= None, tool: str|set[str]|None = None, start: str = None, end: str = None): 
        super().__init__()  # Initialize (replace with appropriate values)
        if not isinstance(institute, str):
            raise ValueError("Institute must be a string for the Activity")
        if person is not None and not isinstance(person, str):
            raise ValueError("Person must be a string or None for the Activity")
        if not isinstance(tool, str, set[str]):
            raise ValueError("Tool must be a string or a set of strings for the Activity")
        if start is not None and not isinstance(start, str):
            raise ValueError("Start Date must be a string or None for the Activity")
        if end is not None and not isinstance(start, str):
            raise ValueError("End Date must be a string or None for the Activity")
        self.institute = institute
        self.person = person
        self.tool = tool         
        self.start = start
        self.end = end
        
    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        return self.person

    def getTools(self):
        return self.tool
    
    def getStartDate(self):
        return self.start 
    
    def getEndDate(self):
        return self.end
    
    def refersTo(self, CulturalObject):     #---->>>non si riferisce a nessun oggetto e non ti ritorna nulla, TI DEVE RITORNARE CULTURAL OBJECT!!
        if isinstance(CulturalObject):
            self.title.append(CulturalObject)
        else:
            raise ValueError("Invalid object type provided")

#Subclass of Activity just with technique parameter

class Acquisition(Activity):
    def _init_(self, technique: str):   
        super().__init__() 
        if not isinstance(technique, str):
            raise ValueError("Acquisition.technique must be a string")
        self.technique = technique
        
    def getTechnique(self):
        return self.technique

#Subclasses without defined parameters
class Processing(Activity):
    pass
        
class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass


class Handler(object):  # Chiara
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


class QueryHandler(Handler):                    #RIVEDERE
    def __init__(self, dbPathOrUrl:str = ""):
       self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str):
        pass


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
    def __init__(self) -> None:
        self.metadataQuery = list()
        self.processQuery = list()
    
    def cleanMetadataHandlers(self) -> bool:    #chiara
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery.clear()  #clear the process handlers list
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler:ProcessDataQueryHandler) -> bool:
        if not isinstance(handler, ProcessDataQueryHandler):
        # check if handler is an istance of processdataqueryhandler (prevention)
            return False
        else:
            return self.processQuery.append(handler)  # Adds a process handler to the list
    
    def getEntityById(self, id: str) -> Optional[IdentifiableEntity]:      #ritorna un oggetto della classe IdentifiableEntity identificando l'entità corrispondente all'identificatore dato nelle basi dati accessibili tramite i gestori di query; se non viene trovata nessuna entità con l'identificatore dato, ritorna None, assicurando che l'oggetto restituito appartenga alla classe appropriata.        
        for handler in self.metadataQuery:
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
        for handler in self.metadataQuery:
            people_data = handler.getAllPeople()
            for _, person_data in people_data.iterrows():
                person = Person(id=person_data['author_id'], name=person_data['author_name'])
                people.append(person)
        return people
    

    

    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:     #restituisce una lista di oggetti della classe CulturalHeritageObject che comprende tutte le entità incluse nel database accessibili tramite i gestori di query, garantendo che gli oggetti nella lista siano della classe appropriata,        
        all_objects = []

        for handler in self.metadataQuery:
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
    
    def getAuthorsOfCulturalHeritageObject(self, id)->list[Person]:  #chiara          
        result = []
        dataf_list = []
        
        for handler in self.metadataQuery:
            dataf_list.append(handler.getAuthorsOfCulturalHeritageObject(id)) 
        dataf_union = pd.concat(dataf_list, ignore_index=True).fillna("")

        for idx, row in dataf_union.iterrows():
            author = row['authorName']
            if author != "":             
                object = Person(id=row["authorID"],name = row['authorName'])
                result.append(object)   
        return result

    def getCulturalHeritageObjectsAuthoredBy(self, person_id: str) -> List[CulturalHeritageObject]:
            if not self.metadataQuery:
                raise ValueError("No metadata query handlers set.")
    
            object_list = []
    
            for handler in self.metadataQuery:
                objects_df = handler.getCulturalHeritageObjectsAuthoredBy(person_id)
        
                for _, row in objects_df.iterrows():
                    id = row['id']
                    title = row['title']
                    date = row.get('date')
                    owner = row['owner']
                    place = row['place']
                    author_name = row['authorName']
                    author_id = row['authorID']
                    author = Person(id=author_id, name=author_name)

                    obj_type = row['type'].split('/')[-1]
                    cultural_obj = None

        
                    if obj_type == 'Map':
                        cultural_obj = Map(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'Painting':
                        cultural_obj = Painting(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'Model':
                        cultural_obj = Model(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'Specimen':
                        cultural_obj = Specimen(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'Herbarium':
                        cultural_obj = Herbarium(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'PrintedMaterial':
                        cultural_obj = PrintedMaterial(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'PrintedVolume':
                        cultural_obj = PrintedVolume(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'ManuscriptVolume':
                        cultural_obj = ManuscriptVolume(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'ManuscriptPlate':
                        cultural_obj = ManuscriptPlate(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    elif obj_type == 'NauticalChart':
                        cultural_obj = NauticalChart(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
                    else:
                        cultural_obj = CulturalHeritageObject(id=id, title=title, owner=owner, place=place, date=date, authors=[author])
            
                    object_list.append(cultural_obj)

            return object_list

        

#ELENA
# mash up = when you fullifll you data model with the data from your queries 
def getAllActivities(self):
    result = []
    handler_list = self.processQuery  # Gets the list of process handlers 
    # process query is an empty variable that collects all the connections to the db
    # 
    df_list = []  # List to store the DataFrames of activities 

    # Loop over each process handler
    # for each process data query handler in each relationsal databases we have, now we just have one, but just in case
    for handler in handler_list:
        df_list.append(handler.getAllActivities())  # Adds the activities of the handler to the list of DataFrames
    # in which class the getallactivities method belong to? to processdataqueryhandler

    # Concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
    # i create a new dataframe with all the df.list(now it is one cause i have one db)
    # drop.duplicates is an operation that removes duplicates
    # na is when you dont have data in row and column --> fillna replaces it with empty cell, prevention of mistake that can break the execution of the software
    # df union is a dataframe -- pd.concat (panda concatenates, even if is just one dataframe)
    # ignore index true - in each df to each row there is an id. if i have more than one df it is a mess because id of each row are the same, 
    # so i ignore the index. i put it here beacuse it is in the step of creaiton of the new df
    # ignore index is the condition. if it says true he does it, if false he odesn't do it

    # Loop over each row of the concatenated DataFrame
    for _, row in df_union.iterrows():
        # _ ignore the indexes and go through each row
        # iterrow goes through each row and its an operation
        # Splits the "internalId" field to get the type and id

    # TROVO ID DELLA ACTIVITY
        activity_type, id = row["internalId"].split("-")
        # in each column i have the activity id so this is how i retrieve the activity type. 
        # Gets the entity referred to by "objectId"
        # to get access to colu8mn internalid and spli type of activity and its id
        obj_refers_to = self.getEntityById(row["objectId"])
        # get access to object id that refers to this specific actiivty by using the getentintybyid method

        # THIS IS THE CONDITIONS
        # Common parameters for all activity objects -- matching data model e existing data from queries according to the method getallactivities
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }
        # after that you fullfill your institue class in your data model
        # match attributes in activity classes of the data model with common params
        # institute= attribute of class in data model
        # row [responsible inst] quindi i add the results of tyhe intersection/cell (okay row but not understandable) of the df to the dm 

        #MATCH ACTIVITY TYPE WITH THE INSTRUCTIONS COMMON PARASMS
        # the conditions are all the ifs
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


def getActivitiesStartedAfter(self, date: str) -> List[Any]:   #Cata
    result = []
    handler_list = self.processQuery  #gets the list of process handlers
    df_list = []  #list to store the DataFrames of activities

    #loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivities())  #adds the activities of the handler to the list of DataFrames

    #concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    #filter activities that started after the given date
    df_filtered = df_union[df_union["start date"] > date]

    #loop over each row of the filtered DataFrame
    for _, row in df_filtered.iterrows():
        # Splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        #common parameters for all activity objects
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        #checks the type of activity and creates the corresponding object
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


def getActivitiesEndedBefore(self, date: str) -> List[Any]:
    result = [] #same process of Started After
    handler_list = self.processQuery  
    df_list = []  

    #loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivities())  # Adds the activities of the handler to the list of DataFrames

    #cncatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    #filter activities that ended before the given date
    df_filtered = df_union[df_union["end date"] < date]

    #loop over each row of the filtered DataFrame
    for _, row in df_filtered.iterrows():
        #splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        # Gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        #common parameters for all activity objects
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "refers_to": obj_refers_to
        }

        #checks the type of activity and creates the corresponding object
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


def getAcquisitionsByTechnique(self, partialName: str) -> List[Any]:
    result = []
    handler_list = self.processQuery  #gets also the list of process handlers
    df_list = []  #empty list to store the DataFrames of activities

    #loop over each process handler
    for handler in handler_list:
        df_list.append(handler.getActivities())  #adds the activities of the handler to the list of DataFrames

    #concatenates all DataFrames, removes duplicates, and fills missing values with empty strings
    df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

    #filter acquisitions that contain the partial name in the technique field
    df_filtered = df_union[df_union["technique"].str.contains(partialName, case=False, na=False)]

    #loop over each row of the filtered DataFrame
    for _, row in df_filtered.iterrows():
        #splits the "internalId" field to get the type and id
        activity_type, id = row["internalId"].split("-")
        #gets the entity referred to by "objectId"
        obj_refers_to = self.getEntityById(row["objectId"])

        # Common parameters for all acquisition objects
        common_params = {
            "institute": row['responsible institute'],
            "person": row['responsible person'],
            "tools": row['tool'],
            "start": row['start date'],
            "end": row['end date'],
            "technique": row['technique'],
            "refers_to": obj_refers_to
        }

        if activity_type == "acquisition":
            object = Acquisition(**common_params)
            result.append(object)

    return result


