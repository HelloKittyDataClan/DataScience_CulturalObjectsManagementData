from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.namespace import FOAF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import csv
import json

from sparql_dataframe import get
from typing import Optional, List, Any, Dict
from pandas import Series
from sqlite3 import connect
from pprint import pp
from os import sep
process = "data" + sep + "process.json"
 

class IdentifiableEntity(object): #identifichiamo l'ID
    def __init__(self, id: str):
        self.id = id 

    def get_id(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, id: str, name: str):
        self.name = name
        super().__init__(id)
    
    def getName(self):
        return self.name


#___________________________CSV_________________________

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, owner: str, place: str, hasAuthor: list[Person], date:str = None):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = hasAuthor 
        super().__init__(id)

    def getTitle(self) ->str:
        return self.title

     
    def getDate(self) -> Optional[str]:
        return self.date

        
    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getAuthors(self) -> list[Person]:
        return self.hasAuthor
    

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


class Activity(object):                               
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refersTo_cho:CulturalHeritageObject):
        self.institute = institute
        self.person = person
        self.tool = tool         
        self.start = start
        self.end = end
        self.refersTo_cho = refersTo_cho
        
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> Optional[str]:
        return self.person
    
    def getTools(self) -> set: # getTool has arity zero or more [0..*]
        return self.tool
    
    def getStartDate(self) -> Optional[str]:
        return self.start

    def getEndDate(self) -> Optional[str]:
        return self.end
    
    def getRefersTo_cho(self) -> CulturalHeritageObject:
        return self.refersTo_cho
 
#Subclass of Activity just with technique parameter
 
class Acquisition(Activity):
    def __init__(self, institute: str, person: str,tool: str|set[str]|None, start: str, end: str, refersTo_cho: CulturalHeritageObject, technique: str):
        super().__init__(institute, person, tool, start, end, refersTo_cho)
        self.technique = technique
    
    def getTechnique(self) -> str:
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass
 
'''
class Activity(object):                               
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        self.institute = institute
        self.person = person
        self.tool = tool         
        self.start = start
        self.end = end
        self.refers_to = refers_to
        
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person == "":
            return None
        else:
            return self.person
 
    def getTools(self):
        return self.tool
    
    def getStartDate(self):
        if self.start == "":
            return None
        else:
            return self.start
    
    def getEndDate(self):
        if self.end == "":
            return None
        else:
            return self.end
    
    def refersTo(self) -> CulturalHeritageObject:   #---->>>non si riferisce a nessun oggetto e non ti ritorna nulla, TI DEVE RITORNARE CULTURAL OBJECT!!
       return self.refers_to
 
#Subclass of Activity just with technique parameter
 
class Acquisition(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, technique: str, refers_to:CulturalHeritageObject):
        self.technique = technique
        super().__init__(institute, person, tool, start, end, refers_to) #for aquisition aggiungiamo il technique quindi la prima volta che lo definiamo è qui, dobbiamo ridichiarare perchè
        if not isinstance(technique, str):
            raise ValueError("Acquisition.technique must be a string")
        
    def getTechnique(self) -> str:
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

#Subclasses with defined parameters
class Processing(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refers_to)
        
class Modelling(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refers_to)
 
class Optimising(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refers_to)
 
class Exporting(Activity):
    def __init__(self, institute: str, person: str, tool: str|set[str]|None, start: str, end: str, refers_to:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refers_to)


#TEST
if __name__ == "__main__":
    # Creazione di un'istanza di Person per gli autori
    author1 = Person("1", "Carracci, Agostino (ULAN:500115349)")

    # Creazione di un oggetto di tipo Painting
    painting = Painting("13", "Portrait of Ulisse Aldrovandi", "Accademia Carrara", "Bergamo", [author1], "1582-1585")

    # Creazione di un'istanza di Acquisition
    acquisition = Acquisition("Council", "Alice Liddell", {"Nikon D7200 Nikor 50mm"}, "2023-03-24", "2023-03-24", "Photogrammetry", refers_to=painting)

    # Accesso agli attributi dell'oggetto Acquisition
    print("Dettagli dell'acquisizione:")
    print(f"Istituto responsabile: {acquisition.getResponsibleInstitute()}")
    print(f"Persona responsabile: {acquisition.getResponsiblePerson()}")
    print(f"Strumenti: {acquisition.getTools()}")
    print(f"Data di inizio: {acquisition.getStartDate()}")
    print(f"Data di fine: {acquisition.getEndDate()}")
    print(f"Tecnica: {acquisition.getTechnique()}")
    print(f"Riferisce a: {acquisition.refersTo().getTitle()}")

    # Creazione di un'istanza di Processing (esempio di sottoclasse senza parametri specifici)
    processing = Processing("Council", "Alice Liddell", {"3DF Zephyr"}, "2023-03-28", "2023-03-29", refers_to=painting)

    # Accesso agli attributi dell'oggetto Processing
    print("\nDettagli dell'attività di processing:")
    print(f"Istituto responsabile: {processing.getResponsibleInstitute()}")
    print(f"Persona responsabile: {processing.getResponsiblePerson()}")
    print(f"Strumenti: {processing.getTools()}")
    print(f"Data di inizio: {processing.getStartDate()}")
    print(f"Data di fine: {processing.getEndDate()}")
    print(f"Riferisce a: {processing.refersTo().getTitle()}")

#-------------- FINE TEST------------------------------------------

'''

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

    

    def pushDataToDb(self, path):
        pass







class MetadataUploadHandler(UploadHandler):  # Chiara
    def __init__(self):  
        super().__init__()

    def pushDataToDb(self, path) -> bool:

            my_graph = Graph()
            # Read the data from the csv file and store them into a dataframe
            venus = pd.read_csv(path,
                                keep_default_na=False,
                                dtype={
                                    "Id": str,
                                    "Type": str,
                                    "Title": str,
                                    "Date": str,
                                    "Author": str,
                                    "Owner": str,
                                    "Place": str,
                                })
            # Rimuoviamo i duplicati della colonna id, mantenendo la prima istanza
            #venus.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)

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

            object_mapping = dict()
            venus.drop_duplicates(subset=["Id"], keep="first", inplace=True, ignore_index=True)

            # Add to the graph the Cultural Object
            for idx, row in venus.iterrows():
                loc_id = "culturalobject-" + str(row["Id"])
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
                        my_graph.add((URIRef(base_url +"culturalobject-" + object_id), relAuthor, author_uri))  # MODIFICA!!! mancava culturalheritageobject come parte del predicato

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
        
        

#---------------------------

# Define the data type for lists of dictionaries
DataType = List[Dict[str, Any]]

# qui c'era class Handler(object):  # Chiara e class UploadHandler(Handler)
# ma non so sto datatype qi sopra dove deve andare


#_____________________RELATIONAL DATA BASE____________________________


class ProcessDataUploadHandler(UploadHandler):  #Cata
    def __init__(self):
        super().__init__()


    #Create a function to create 5 different tables
    def pushDataToDbActivities(self, file_path: str, field_name: str) -> pd.DataFrame:
        # Load JSON data from file
        with open(file_path, 'r') as file:
            df_activity = json.load(file)
        
        table_data: List[Dict[str, Any]] = []
        
        for item in df_activity:
            if field_name in item:
                field_entry = item[field_name]
                field_entry['object id'] = item['object id']
                table_data.append(field_entry)

        # Convert the list of dictionaries to a DataFrame
        df_activities = pd.DataFrame(table_data)
        
        # Convert lists in 'tool' column to comma-separated strings because lists are not readed
        if 'tool' in df_activities.columns:
            df_activities['tool'] = df_activities['tool'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            
        return df_activities

    #Create internalId for each one (important for queries)
    def addInternalIds(self, df: pd.DataFrame, field_name: str) -> pd.DataFrame:
        # Create internal IDs
        internal_ids = [f"{field_name}-{idx}" for idx in range(len(df))]
        # Add the internal IDs as a new column
        df.insert(0, "internalId", Series(internal_ids, dtype="string"))
        
        return df

    #Replace object id with objectId (internal id of objects). Two cases with the row technique included (activities) or without this data
    def extractAndRenameColumns(self, df: pd.DataFrame, include_technique: bool = False) -> pd.DataFrame:
        columns = ["internalId", "object id", "responsible institute", "responsible person", "tool", "start date", "end date"]
        if include_technique:
            columns.insert(4, "technique")  # Insert 'technique' column before 'tool'
        
        identifiers = df[columns]
        identifiers = identifiers.rename(columns={"object id": "objectId"})
        return identifiers
        
    #Create individual DataFrame tables calling the pushDataToDbActivities, internal ID, etc.
    def pushDataToDb(self, activities_file_path: str):
        #Create individual DataFrames
        acquisition_df = self.pushDataToDbActivities(activities_file_path, 'acquisition')
        processing_df = self.pushDataToDbActivities(activities_file_path, 'processing')
        modelling_df = self.pushDataToDbActivities(activities_file_path, 'modelling')
        optimising_df = self.pushDataToDbActivities(activities_file_path, 'optimising')
        exporting_df = self.pushDataToDbActivities(activities_file_path, 'exporting')

        #Add internal IDs to each DataFrame
        acquisition_df = self.addInternalIds(acquisition_df, 'acquisition')
        processing_df = self.addInternalIds(processing_df, 'processing')
        modelling_df = self.addInternalIds(modelling_df, 'modelling')
        optimising_df = self.addInternalIds(optimising_df, 'optimising')
        exporting_df = self.addInternalIds(exporting_df, 'exporting')
        
        #Extract and rename columns, including 'technique' for acquisition
        acquisition_final_db = self.extractAndRenameColumns(acquisition_df, include_technique=True)
        processing_final_db = self.extractAndRenameColumns(processing_df)
        modelling_final_db = self.extractAndRenameColumns(modelling_df)
        optimising_final_db = self.extractAndRenameColumns(optimising_df)
        exporting_final_db = self.extractAndRenameColumns(exporting_df)
        
        #Save to SQLite database in the file
        with connect("relational.db") as con:
            acquisition_final_db.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_final_db.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_final_db.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_final_db.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_final_db.to_sql("Exporting", con, if_exists="replace", index=False)
        
        return True



class QueryHandler(Handler):
    def __init__(self,):  
        super().__init__()
    

    def getById(self, id: str) -> pd.DataFrame:
        id = str(id)
        grp_endpoint = "http://127.0.0.1:9999/blazegraph/"

    
        if id.isdigit():
            query = """
            PREFIX FOAF: <http://xmlns.com/foaf/0.1>
            PREFIX schema: <http://schema.org/>
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {
                ?object <http://schema.org/identifier> "%s" .
                ?object <http://schema.org/identifier> ?id .
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
                ?object <http://schema.org/title> ?title .
                ?object <http://github.com/HelloKittyDataClan/DSexam/owner> ?owner .
                ?object <http://schema.org/itemLocation> ?place .
             
                OPTIONAL {?object <http://schema.org/dateCreated> ?date .}
                OPTIONAL {?object <http://schema.org/author> ?author .}
                OPTIONAL {?author <http://xmlns.com/foaf/0.1/name> ?author_name .}
                OPTIONAL {?author <http://schema.org/identifier> ?author_id .}
                
            }
            """ % id
        else:
            query = """
            PREFIX FOAF: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>
            SELECT DISTINCT ?uri ?name ?id 
            WHERE {
                ?uri <http://schema.org/identifier> "%s" ;
                     <http://xmlns.com/foaf/0.1/name> ?name ;
                     <http://schema.org/identifier> ?id .
                ?object <http://schema.org/author> ?uri .
            }
            """ % id
    
        results = get(grp_endpoint, query, True)
        return results




class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
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
        results = get(self.dbPathOrUrl + "sparql",query, True)
        return results

    
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
        results = get(self.dbPathOrUrl + "sparql", query, True)
        return results       
    

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
        results = get(self.dbPathOrUrl + "sparql",query, True)
        return results
    
    

    
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
        results = get(self.dbPathOrUrl + "sparql",query, True)
        return results


#_____________QUERIES_____________________________#elena

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities(self):
        try:
            # Modify the partialName parameter to include "wildcard characters" for partial matching
            # nell'input se inserisco anche solo un risultato parziale mi compare comunque
            with connect(self.getDbPathOrUrl()) as con: # metodo ereditato dal query handler, posso connettere al path del relational database graze al getDbPathOrUrl 
                # with / as = construction from panda
               # adds every colums from every table 
                query = """
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    UNION 
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                """
                # pd = panda
                # pd.read_sql(query, con) executes the SQL query against the database using the connection con and returns the result as a pandas DataFrame (result of a query)
                result = pd.read_sql(query, con)
                return result
        except Exception as e:
            print(f"An error occurred: {e}")

    def getActivitiesByResponsibleInstitution(self, partialName: str): 
        # questo è un metodo
        #partialName lo da peroni come parametro?, è la stringa di input
        try:
            # Modify the partialName parameter to include "wildcard characters" for partial matching
            # nell'input se inserisco anche solo un risultato parziale mi compare comunque

            partial_name = f"%{partialName}%"
            # allow me to do the partial match
            
            # Connect to the database #modific con la parte di catalina
            with connect(self.getDbPathOrUrl()) as con:
                # Define the SQL query with placeholders for parameters, quindi seleziono tutte le colonne
                # con il FROM indico da che ? .... tabella
                # con il where da che colonna
                query2 = """
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                FROM acquisition
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM processing
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM modelling
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM optimising
                WHERE "responsible institute" LIKE ?
                UNION
                SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                FROM exporting
                WHERE "responsible institute" LIKE ?
                """
                # Execute the query with the provided parameters
                # ? = filtration according to the input, non ce l'ho in getallactivities perchè mi riporta tutte le attività
                # qui ce l'ho perchè voglio info solo da un tipo di colonna specifica del db che è responsible institute
                query2_table = pd.read_sql(query2, con, params=[partial_name]*5)
                # The params argument is a list of parameters to be used in the SQL query. Since there are 5 ? placeholders in the query (one for each UNION segment), the list [partial_name]*5 is used to provide the same partial_name parameter for each placeholder.
                return query2_table
            #  The result of the query is returned as a pandas DataFrame and assigned to the variable query2_table.
        except Exception as e:
            print("An error occurred:", e)
            return None

    def getActivitiesByResponsiblePerson(self, partialName: str):
            try:
                partial_name = f"%{partialName}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query3 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "responsible person" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "responsible person" LIKE ?
                    """
                    query3_table = pd.read_sql(query3, con, params= [partial_name]*5)
                    return query3_table
            except Exception as e:
                print("An error occurred:", e)
                
    def getActivitiesUsingTool(self, partialName: str):
            try:
                partial_name= f"%{partialName}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query4 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE tool LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE tool LIKE ?
                    """
                    query4_table = pd.read_sql(query4, con, params=[partial_name]*5)
                    return query4_table
            except Exception as e:
                print("An error occurred:", e)
                
    def getActivitiesStartedAfter (self, date: str):
            try:
                date=f"%{date}%"
                with connect(self.getDbPathOrUrl()) as con:
                    query5 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "start date" LIKE ?
                    UNION
                    -- con l'union faccio 1 +1 con
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "start date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "start date" LIKE ?
                    """
                    query5_table = pd.read_sql(query5, con, params=[date]*5)
                    return query5_table
            except Exception as e:
                print ("An error occured:", e)

    def getActivitiesEndedBefore(self, date: str):
            try:
                date=f"%{date}%"
                with connect (self.getDbPathOrUrl()) as con:
                    query6 = """
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    WHERE "end date" LIKE ?
                    UNION  
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM processing
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM modelling
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    WHERE "end date" LIKE ?
                    UNION
                    SELECT internalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                    WHERE "end date" LIKE ?
                    """
                    query6_table = pd.read_sql(query6, con, params=[date]*5)
                    return query6_table
            except Exception as e:
                print ("An error occured:", e) 

    def getAcquisitionsByTechnique(self, partialName: str):
        try:
            partial_name = f"%{partialName}%"
            
            with connect(self.getDbPathOrUrl()) as con:
                query = "SELECT * FROM acquisition WHERE technique LIKE ?"
                query_table = pd.read_sql(query, con, params=[partial_name])
                return query_table
        except Exception as e:
            print("An error occurred:", e)
            return None

process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("relational.db")


'''
#____________________TESTS QUERIES_____________________
upload = ProcessDataUploadHandler()
upload.setDbPathOrUrl("relational.db")
upload.pushDataToDb("data/process.json")

process_query_handler = ProcessDataQueryHandler()
process_query_handler.setDbPathOrUrl("relational.db")


# for mashup
mashup = BasicMashup()
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("relational.db")

# Adding process query handler to mashup
mashup.addProcessHandler(process_query)'''

'''# Calling the method 1 = 35 x 5
partial_name_person = "Grace"
activities_by_person = process_query_handler.getActivitiesByResponsiblePerson(partial_name_person)
all_activities = process_query_handler.getAllActivities()
import tabulate
#print(tabulate.tabulate(all_activities,headers="keys"))
print(tabulate.tabulate(activities_by_person,headers="keys"))

# Call the method 2
partial_name_person = "Grace"
activities_by_person = process_query_handler.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities by tool '{partial_name_person}':")
for person in activities_by_person:
    print(person)
    

# Call the method 3
partial_name_tool = "Blender"
activities_by_tool = process_query_handler.getActivitiesUsingTool(partial_name_tool)
print(f"Activities by tool '{partial_name_tool}':")
for activity in activities_by_tool:
    print(activity)


partial_name_person = "Blender"
activities_by_person = process_query_handler.getActivitiesUsingTool(partial_name_person)
import tabulate
#print(tabulate.tabulate(all_activities,headers="keys"))
print(tabulate.tabulate(activities_by_person,headers="keys"))

# Call the method 4
partial_name_institution = "Philology"
activities_by_institution = mashup.getActivitiesByResponsibleInstitution(partial_name_institution)
print(f"Activities by institution '{partial_name_institution}':")

for activity in activities_by_institution:
    print(activity.getResponsibleInstitute())


partial_name_person = "Philology"
activities_by_person = process_query_handler.getActivitiesByResponsibleInstitution(partial_name_person)
import tabulate
#print(tabulate.tabulate(all_activities,headers="keys"))
print(tabulate.tabulate(activities_by_person,headers="keys"))


# Call the method 5
partial_name_start = "2023"
activities_by_start = mashup.getActivitiesStartedAfter(partial_name_start)
print(f"Activities by start date '{partial_name_start}':")
for activity in activities_by_start:
    print(activity)

partial_name_person = "2023-02-10"
activities_by_person = process_query_handler.getActivitiesEndedBefore(partial_name_person)
import tabulate
#print(tabulate.tabulate(all_activities,headers="keys"))
print(tabulate.tabulate(activities_by_person,headers="keys"))


# Call the method 6
partial_name_end = "2023-02-10"
activities_by_end = mashup.getActivitiesEndedBefore(partial_name_end)
print(f"Activities by end date '{partial_name_end}':")
for activity in activities_by_end:
    print(activity)

# Call the method 7
partial_name_technique = "Structured-light 3D scanner"
activities_by_technique = mashup.getAcquisitionsByTechnique(partial_name_technique)
print(f"Activities by technique '{partial_name_technique}':")
for activity in activities_by_technique:
    print(activity)
    

partial_name_person = "Structured-light 3D scanner"
activities_by_person = process_query_handler.getAcquisitionsByTechnique(partial_name_person)
import tabulate
#print(tabulate.tabulate(all_activities,headers="keys"))
print(tabulate.tabulate(activities_by_person,headers="keys"))'''


#BasicMashup

class BasicMashup(object):
    def __init__(self) -> None:
        self.metadataQuery = list()
        self.processQuery = list()
    
    def cleanMetadataHandlers(self) -> bool:    #chiara
        self.metadataQuery = []
        return True

    def cleanProcessHandlers(self) -> bool:      #cata
        self.processQuery.clear()  #clear the process handlers list
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:     #bea
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler:'ProcessDataQueryHandler') -> bool:   #elena
        if not isinstance(handler, ProcessDataQueryHandler):
        # check if handler is an istance of processdataqueryhandler (prevention)
            return False
        else:
            self.processQuery.append(handler)  # Adds a process handler to the list
            return True
        

        
    def getEntityById(self, related_id: str):
        if not self.metadataQuery:
            return None

        for handler in self.metadataQuery:
            entity_df = handler.getById(related_id)

            if entity_df.empty:
                continue

            row = entity_df.loc[0]

            if not related_id.isdigit():
                person_uri = related_id  # L'ID della persona
                result = Person(person_uri, row["name"])
                return result  # Restituisce l'oggetto Person immediatamente

            list_of_authors = []
            authors = self.getAuthorsOfCulturalHeritageObject(related_id)

            for author in authors:
                author_obj = Person(author.id, author.getName())
                list_of_authors.append(author_obj)
            
            base_url = "http://github.com/HelloKittyDataClan/DSexam/"

            if row["type"] == base_url + "NauticalChart":
                new_object = NauticalChart(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            elif row["type"] == base_url + "ManuscriptPlate":
                new_object = ManuscriptPlate(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            elif row["type"] == base_url + "ManuscriptVolume":
                new_object = ManuscriptVolume(related_id, row["title"], row["owner"], row["place"], list_of_authors, row["date"])
            elif row["type"] == base_url + "PrintedVolume":
                new_object = PrintedVolume(related_id, row["title"], row["owner"], row["place"], list_of_authors, row["date"],)
            elif row["type"] == base_url + "PrintedMaterial":
                new_object = PrintedMaterial(related_id, row["title"], row["owner"], row["place"], list_of_authors, row["date"])
            elif row["type"] == "https://dbpedia.org/property/Herbarium":
                new_object = Herbarium(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            elif row["type"] == base_url + "Specimen":
                new_object = Specimen(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            elif row["type"] == "https://dbpedia.org/property/Painting":
                new_object = Painting(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            elif row["type"] == "https://dbpedia.org/property/Model":
                new_object = Model(related_id, row["title"], row["owner"], row["place"], list_of_authors, row["date"])
            elif row["type"] == "https://dbpedia.org/property/Map":
                new_object = Map(related_id, row["title"], row["owner"], row["place"],list_of_authors, row["date"])
            else:
                continue  # Passa all'handler successivo se il tipo dell'oggetto non corrisponde a nessuno dei tipi specificati

            return new_object
        

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
                    hasAuthor=row.get('authors', [])
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

    def getCulturalHeritageObjectsAuthoredBy(self, person_id: str) -> List[CulturalHeritageObject]:     #leggera modifica 
        if not self.metadataQuery:
            raise ValueError("No metadata query handlers set.")
    
        object_list = []
    
        for handler in self.metadataQuery:
            objects_df = handler.getCulturalHeritageObjectsAuthoredBy(person_id)
        
            for _, row in objects_df.iterrows():
                id = str(row['id'])  # Ensure ID is a string
                title = row['title']
                date = row.get('date')
                if date is not None and not isinstance(date, str):
                    date = str(date)  # Convert date to string if it's not None and not already a string
                owner = row['owner']
                place = row['place']
                author_name = row['authorName']
                author_id = str(row['authorID'])  # Ensure author ID is a string
                author = Person(id=author_id, name=author_name)

                obj_type = row['type'].split('/')[-1]
                cultural_obj = None
            
           
                if obj_type == 'NauticalChart':
                    cultural_obj = NauticalChart(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'ManuscriptPlate':
                    cultural_obj = ManuscriptPlate(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'ManuscriptVolume':
                    cultural_obj = ManuscriptVolume(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'PrintedVolume':
                    cultural_obj = PrintedVolume(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'PrintedMaterial':
                    cultural_obj = PrintedMaterial(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Herbarium':
                    cultural_obj = Herbarium(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Specimen':
                    cultural_obj = Specimen(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Painting':
                    cultural_obj = Painting(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Model':
                    cultural_obj = Model(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                elif obj_type == 'Map':
                    cultural_obj = Map(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
                else:
                    cultural_obj = CulturalHeritageObject(id=id, title=title, owner=owner, place=place, date=date, hasAuthor=[author])
            
                object_list.append(cultural_obj)

        return object_list
        

#ELENA
# mash up = when you fullifll you data model with the data from your queries 
    def getAllActivities(self) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        if not df_list:
            return pd.DataFrame()

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])
            

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesByResponsiblePerson(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsiblePerson(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesUsingTool(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesStartedAfter(self, date: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        df_filtered = df_union[df_union["start date"] > date]

        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result

    def getActivitiesEndedBefore(self, date: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities())

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        df_filtered = df_union[df_union["end date"] < date]

        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "processing":
                object = Processing(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "modelling":
                object = Modelling(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "optimising":
                object = Optimising(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)

            elif activity_type == "exporting":
                object = Exporting(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                refersTo_cho=obj_refers_to )
                result.append(object)


        return result

    def getAcquisitionsByTechnique(self, partialName: str) -> List[Any]:
        result = []
        handler_list = self.processQuery
        df_list = []

        # Collect activities from handlers
        for handler in handler_list:
            activities = handler.getAllActivities()
            if activities is not None and not activities.empty:
                df_list.append(activities)
            else:
                print(f"No activities found in handler: {handler}")

        # Check if df_list is not empty
        if not df_list:
            print("No activities found in any handler.")
            return result

        # Concatenate DataFrames
        try:
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
        except ValueError as e:
            print(f"Error concatenating DataFrames: {e}")
            return result

        # Filter acquisitions by technique
        try:
            df_filtered = df_union[df_union["technique"].str.contains(partialName, case=False, na=False)]
        except KeyError as e:
            print(f"Key error: {e}")
            return result

        # Process each filtered row
        for _, row in df_filtered.iterrows():
            activity_type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if activity_type == "acquisition":
                object = Acquisition(institute=row['responsible institute'],
                person=row['responsible person'],
                tool=row['tool'],
                start=row['start date'],
                end=row['end date'],
                technique=row['technique'],
                refersTo_cho=obj_refers_to )
                result.append(object)

        return result



#__________________________ TESTS BASIC MASHUP___________________________

#mashup = AdvancedMashup()
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("relational.db")
"""
# Adding process query handler to mashup
mashup.addProcessHandler(process_query)

# Calling the method 1
all_activities = mashup.getAllActivities()
for i in all_activities:
    print( i.getResponsibleInstitute(), i.getResponsiblePerson(), i.getTools(), i.getStartDate(), i.getEndDate())


i.refersTo().id,
# Call the method 2
partial_name_person = "Hopper"
activities_by_person = mashup.getActivitiesByResponsiblePerson(partial_name_person)
for i in activities_by_person:
    print(f"Activities by person '{partial_name_person}':")

  
# Call the method 2
partial_name_person = "Grace"
activities_by_person = mashup.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities by tool '{partial_name_person}':")
for person in activities_by_person:
    print(person)



# Call the method 3
partial_name_tool = "Blender"
activities_by_tool = mashup.getActivitiesUsingTool(partial_name_tool)
print(f"Activities by tool '{partial_name_tool}':")
for activity in activities_by_tool:
    print(activity.institute)


# Call the method 4
partial_name_institution = "Philology"
activities_by_institution = mashup.getActivitiesByResponsibleInstitution(partial_name_institution)
print(f"Activities by institution '{partial_name_institution}':")
for activity in activities_by_institution:
    print(activity)

# Call the method 5
partial_name_start = "2023"
activities_by_start = mashup.getActivitiesStartedAfter(partial_name_start)
print(f"Activities by start date '{partial_name_start}':")
for activity in activities_by_start:
    print(activity)

# Call the method 6
partial_name_end = "2023-02-10"
activities_by_end = mashup.getActivitiesEndedBefore(partial_name_end)
print(f"Activities by end date '{partial_name_end}':")
for activity in activities_by_end:
    print(activity)

# Call the method 6
partial_name_technique = "Structured-light 3D scanner"
activities_by_technique = mashup.getAcquisitionsByTechnique(partial_name_technique)
print(f"Activities by technique '{partial_name_technique}':")
for activity in activities_by_technique:
    print(activity)


"""
class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()

    #chiara --  restituire una lista di oggetti di tipo CulturalHeritageObject che sono stati gestiti da una responsabile persona
    def getObjectsHandledByResponsiblePerson(self, partName: str) -> list[CulturalHeritageObject]:
        obj_id = set()   #per memorizzare gli ID degli oggetti rilevanti dalle attività gestite dalla persona responsabile.
    
        for activity in self.getActivitiesByResponsiblePerson(partName):  #iteriamo sulle attività gestite dal metodo getActivitiesByResponsiblePerson
            obj_id.add(activity.getRefersTo_cho().id)   # Per ogni attività, activity.refersTo() restituisce l'oggetto a cui l'attività fa riferimento. 
                                                    #.id viene utilizzato per ottenere l'ID dell'oggetto e questo ID viene aggiunto all'insieme obj_id.
    
        cultural_objects = self.getAllCulturalHeritageObjects() # itero direttamente sugli oggetti culturali filtrando gli oggetti in base agli ID memorizzati nell'insieme obj_id.
        obj_list = []


        for obj in cultural_objects:    # Per ogni oggetto, verifica se l'ID dell'oggetto è presente nell'insieme obj_id. Se è presente, aggiungi l'oggetto alla lista
            if obj.id in obj_id:
                obj_list.append(obj)

        return obj_list   #che contiene tutti gli oggetti culturali gestiti dalla persona responsabile
    


    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> List[CulturalHeritageObject]:  #BEA
        matched_objects = []
        self.activities = []  # Lista delle attività

        # Itera su tutte le attività
        for activity in self.activities:
            # Controlla se l'attività è gestita dall'istituzione responsabile specificata (anche parzialmente)
            if partialName.lower() in activity.getResponsibleInstitute().lower():  
                # Recupera l'oggetto culturale a cui si riferisce l'attività
                cultural_heritage_object = activity.getRefersTo_cho()
                # Verifica il tipo dell'oggetto culturale e aggiungilo alla lista se è appropriato
                if isinstance(cultural_heritage_object, CulturalHeritageObject):
                    matched_objects.append(cultural_heritage_object)

        return matched_objects
        

    
    def getActivitiesOnObjectsAuthoredBy(self, personId: str):
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        id_list = []
        for cultural_object in cultural_objects:
            object_id = cultural_object.id
            id_list.append(object_id)
        activities = self.getAllActivities()
        result_list = []
        for activity in activities:

            referred_object = activity.getRefersTo_cho()
            referred_object_id = referred_object.id
            if referred_object_id in id_list:
                result_list.append(activity)
        return result_list
    

    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str):
        activities_started_after_start = self.getActivitiesStartedAfter(start)
        acquisition_start = []
        for activity in activities_started_after_start:
            if type(activity) is Acquisition:
                referred_object_id = (activity.getRefersTo_cho()).id
                acquisition_start.append(referred_object_id)
        activities_ended_before_end = self.getActivitiesEndedBefore(end)
        acquisition_end = []
        for activity in activities_ended_before_end:
            if type(activity) is Acquisition:
                referred_object_id = (activity.getRefersTo_cho()).id
                acquisition_end.append(referred_object_id)
        acquisition_list = []
        for obj_id in acquisition_start:
            if obj_id in acquisition_end:
                acquisition_list.append(obj_id)
        authors_of_obj = set()
        for obj_id in acquisition_list:
            authors = self.getAuthorsOfCulturalHeritageObject(str(obj_id))
            for author in authors:
                if author is not None:
                    authors_of_obj.add((author.id, author.name))
        authors_list = []
        for author_tuple in authors_of_obj:
            person = Person(id=author_tuple[0], name=author_tuple[1])
            authors_list.append(person)
        return authors_list

'''
mashup = AdvancedMashup()
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("relational.db")
process_handler= ProcessDataQueryHandler()
metadata_handler =  MetadataQueryHandler()
metadata_handler.setDbPathOrUrl("http://192.168.1.151:9999/blazegraph/")
process_handler.setDbPathOrUrl("relational.db")

mashup.addMetadataHandler(metadata_handler)
mashup.addProcessHandler(process_query)

#print(metadata_handler.getById("1"))
import tabulate
print(tabulate.tabulate(metadata_handler.getById("7"),headers="keys"))


# Usage example:
 
# Create an instance of BasicMashup
mashup = BasicMashup()
 
# Create an instance of MetadataQueryHandler with a specific endpoint URL
handler = MetadataQueryHandler("http://192.168.1.63:9999/blazegraph/")
 
# Add the metadata query handler to mashup
mashup.addMetadataHandler(handler)
 
# ID of the entity to retrieve (replace with a real ID)
entity_id = "1"
 
# Call getEntityById to retrieve an entity by ID
entity = mashup.getEntityById(entity_id)
 

print (entity.id)

--------------

#metterli
pippo = MetadataUploadHandler()
output = pippo.setDbPathOrUrl(" http://192.168.1.151:9999/blazegraph/")
#prendere
output = pippo.pushDataToDb("data/meta.csv")
print(output)

db_url = ("http://192.168.1.151:9999/blazegraph/")
boh = QueryHandler(dbPathOrUrl=db_url)



#cercare
topolino = MetadataQueryHandler()
output = topolino.setDbPathOrUrl(" http://192.168.1.151:9999/blazegraph/")

masha = BasicMashup()
masha.metadataQuery = [topolino]
print(masha.getEntityById(id=4))

#for bb in masha.getAuthorsOfCulturalHeritageObject(id=18):
#    print(bb.name, bb.id)




'''
 
#------------------TEST-----------------------------

rel_path = "relational.db"
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")

#metterli
pippo = MetadataUploadHandler()  # Passa l'argomento richiesto qui
output = pippo.setDbPathOrUrl("http://192.168.1.69:9999/blazegraph/")
#prendere
output = pippo.pushDataToDb("data/meta.csv")
print(output)

process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

#cercare
topolino = MetadataQueryHandler()  # Passa l'argomento richiesto qui
output = topolino.setDbPathOrUrl("http://192.168.1.69:9999/blazegraph/")

masha = BasicMashup()
masha.metadataQuery = [topolino]
pp(masha.getAuthorsOfCulturalHeritageObject(id=4))

#for bb in masha.getAuthorsOfCulturalHeritageObject(id=18):
#    print(bb.name, bb.id)

#mashup = AdvancedMashup()
#mashup.addProcessHandler(process_qh)
#mashup.addMetadataHandler(pippo)

#result = mashup.getObjectsHandledByResponsibleInstitution("Philology")
#pp(result)



#TEST FINALI DI VALENTINO
'''
rel_path = "databases/relational.db"
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb("data/process.json")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

# Then, create the graph database (remember first to run the Blazegraph instance) using the related source data
blaz_url = "http://127.0.0.1:9999/blazegraph/" # copy-paste url appearing when the blazegraph instance is run
grp_endpoint =  blaz_url + "sparql"
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_endpoint)
metadata.pushDataToDb("data/meta.csv")
# Please remember that one could, in principle, push one or more files calling the method one or more times (even calling the method twice specifying the same file!)

# In the next passage, create the query handlers for both the databases, using the related classes
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl(grp_endpoint)

# Finally, create a advanced mashup object for asking about dataclear
mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)
mashup.addMetadataHandler(metadata_qh)

result_q1 = mashup.getEntityById("22")
print(result_q1.__dict__["authors"][0].__dict__)
print(f"method getEntityById, wrong input: {result_q1}\n")
result_q2 = mashup.getEntityById("33")
print(f"method getEntityById Object: {result_q2}\n")
result_q3 = mashup.getEntityById("VIAF:100190422")
print(f"method getEntityById author: {result_q3}\n")

result_q4 = mashup.getAllPeople()
print(f"method getAllPeople:{result_q4}\n")

result_q5 = mashup.getAllCulturalHeritageObjects()
print(f"method getAllCulturalHeritageObjets: {result_q5}\n")
result_q6 = mashup.getAuthorsOfCulturalHeritageObject("17")
print(f"method getAuthorsOfCulturalHeritageObject: {result_q6}")
result_q7 = mashup.getAuthorsOfCulturalHeritageObject("45")
print(f"method getAuthorsOfCulturalHeritageObject wrong input: {result_q7}\n")

result_q8 = mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:100203985")
print(f"method getCulturalHeritageObjectsAuthoredBy: {result_q8}\n")
result_q9 = mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:1")
print(f"method getCulturalHeritageObjectsAuthoredBy wrong input: {result_q9}\n")

result_q10 = mashup.getAllActivities()
print(f"method getAllActivities: {result_q10}\n")

result_q11 = mashup.getActivitiesByResponsibleInstitution("Heritage")
print(f"method getActivitiesByResponsibleInstitution: {result_q11}\n")
result_q12 = mashup.getActivitiesByResponsibleInstitution("Lidl")
print(f"method getActivitiesByResponsibleInstitution, wrong input: {result_q12} \n")

result_q12 = mashup.getActivitiesByResponsiblePerson("Hopper")
print(f"method getActivitiesByResponsiblePerson: {result_q12}\n")
result_q13 = mashup.getActivitiesByResponsiblePerson("Hitler")
print(f"method getActivitiesByResponsiblePerson, wrong input:{result_q13}\n")

result_q14 = mashup.getActivitiesUsingTool("Nikon")
print(f"method getActivitiesUsingTool: {result_q14}\n")
result_q15 = mashup.getActivitiesUsingTool("Zappa")
print(f"method getActivitiesUsingTool, wrong input: {result_q15}\n")

result_q16 = mashup.getActivitiesStartedAfter("2023-08-21")
print(f"method getActivitiesStartedAfter: {result_q16}\n")
result_q17 = mashup.getActivitiesStartedAfter("2028-01-01")
print(f"method getActivitiesStartedAfter, wrong input: {result_q17}\n")

result_q18 = mashup.getActivitiesEndedBefore("2023-06-10")
print(f"method getActivitiesEndedBefore: {result_q18}\n")
result_q19 = mashup.getActivitiesEndedBefore("2028-01-01")
print(f"method getActivitiesEndedBefore, wrong input: {result_q19}\n")

result_q20 = mashup.getAcquisitionsByTechnique("Photogrammetry")
print(f"method getAcquisitionsByTechnique: {result_q20}\n")
result_q21 = mashup.getAcquisitionsByTechnique("Zappa")
print(f"method getAcquisitionsByTechnique, wrong input: {result_q21}\n")

print("--- AdMash ---\n")

result_q22 = mashup.getActivitiesOnObjectsAuthoredBy("VIAF:100190422")
print(f"method getActivitiesOnObjectsAuthoredBy: {result_q22}\n")
result_q23 = mashup.getActivitiesOnObjectsAuthoredBy("VIAF:1")
print(f"method getActivitiesOnObjectsAuthoredBy, wrong input: {result_q23}\n")

result_q24 = mashup.getObjectsHandledByResponsiblePerson("Jane")
print(f"method getObjectsHandledByResponsiblePerson: {result_q24}\n")
result_q25 = mashup.getObjectsHandledByResponsiblePerson("Chicoria")
print(f"method getObjectsHandledByResponsiblePerson, wrong input: {result_q25}\n")

result_q26 = mashup.getObjectsHandledByResponsibleInstitution("Heritage")
print(f"method getObjectsHandledByResponsibleInstitution: {result_q26}\n")
result_q27 = mashup.getObjectsHandledByResponsibleInstitution("Lidl")
print(f"method getObjectsHandledByResponsibleInstitution, wrong input: {result_q27}\n")

result_q28 = mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-03-10", "2023-11-10")
print(f"method getAuthorsOfObjectsAcquiredInTimeFrame: {result_q28}\n")
result_q29 = mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2028-01-01", "2029-01-01")
print(f"method getAuthorsOfObjectsAcquiredInTimeFrame, wrong input: {result_q29}\n")

'''