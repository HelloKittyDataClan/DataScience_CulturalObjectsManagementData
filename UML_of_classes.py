from rdflib import Namespace 
from rdflib.namespace import FOAF
from rdflib import Graph, URIRef, RDF, Namespace, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import pandas as pd

import csv



class Handler:                             #Chiara
    def __init__(self): 
        self.dbPathOrUrl = "" 

    def getDbPathOrUrl(self): 
        return self.dbPathOrUrl  
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:   
        if isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        else:
            return False

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass

class MetadataUploadHandler(UploadHandler):         #Chiara
    def __init__(self):
        self.dbPathOrUrl = ""


    def pushDataToDb(self, path):
        my_graph = Graph()
        #read the data from the csv file and store them into a dataframe --
        venus = pd.read_csv("../data/meta.csv",
                keep_default_na=False,
                dtype={
                "Id": "string",
                "Type": "string",
                "Title": "string",
                "Date": "string",
                "Author": "string",
                "Owner": "string",
                "Place": "string",
            },
        )
    
        # Define namespaces
        base_url = Namespace("http://github.com/HelloKittyDataClan/DSexam/") #  #our base url --
        db = Namespace ("http//dbpedia.org/resource/")
        schema = Namespace ("http://schema.org/")

        # Create Graph
        my_graph.bind("base_url", base_url)
        my_graph.bind("dpedia", db)
        my_graph.bind("FOAF", FOAF)
        my_graph.bind ("schema", schema)
        
      
        # Define classes about Cultural Object ---
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
        date = URIRef(schema + "dataCreated")
        place = URIRef(schema + "itemLocation")
        id = URIRef(schema + "identifier")
        owner = URIRef(base_url + "owner")

        # Relation with authors among classes
        relAuthor = URIRef(schema + "author")

        # Attributes related to the class Person
        name = URIRef(FOAF + "name")

 # Add to the graph the Cultural Object
        for idx, row in venus.iterrows():
            loc_id = "culturalobject-" + str(idx)
            subj = URIRef(base_url + loc_id)

            #assign a resource classes to the object
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


        # Dictionaries to track authors and associated objects
        author_id_mapping = dict()   
        object_mapping = dict()
        people_counter = 0

        for idx, row in venus.iterrows():
            # Extract information about authors
            if row["Author"] != "":
                author_list = row["Author"].strip('\"').split(";")   # Split author string into a list of authors
                for author in author_list:
                    author_st = author.strip()
                    indx_id = author_st.find("(")   # Find the index of the opening parenthesis 
                    author_name = author_st[:indx_id-1]  # Extract the author name from the beginning of the string up to the opening parenthesis
                    author_id = author_st[(indx_id+1):-1]   # Extract the id related to the author's name
                    
                    obj_id = row["Id"]
                    # Check if the author ID is already in the author_id_mapping dictionary
                    if author_id in author_id_mapping.keys():
                      author_uri = author_id_mapping[author_id]
                      if obj_id in object_mapping.keys():
                        object_mapping[obj_id].append(author_uri)  # append author uri
                      else:
                          object_mapping[obj_id] = [author_uri]       # initialize a new list with the author URI
                    else:
                    #generate a new uri
                        loc_author_id = "Person/" + str(people_counter)
                        subj_author = URIRef(base_url + loc_author_id)


                 # adding to the graph the type, id and the name of the person
                my_graph.add((subj_author, RDF.type, Person))
                my_graph.add((subj_author, id, Literal(str(author_id))))
                my_graph.add((subj_author, name, Literal(str(author_name))))
                
               # Manage author duplicates and ensure each author has a unique ID
                author_id_mapping[author_id] = subj_author
                if obj_id in object_mapping.keys():
                    object_mapping[obj_id].append(subj_author)
                else:
                    object_mapping[obj_id] = [subj_author]

                people_counter += 1

                #assign identifier
                if row["Id"] != "":
                    my_graph.add((subj, id, Literal(str(row["Id"]))))
            #assign title
                if row["Title"] != "":
                    my_graph.add((subj, title, Literal(str(row["Title"]))))

            #assign date
                if row["Date"] != "":
                    my_graph.add((subj, date, Literal(str(row["Date"]))))

            #assign owner
                if row["Owner"] != "":
                    my_graph.add((subj, owner, Literal(str(row["Owner"]))))

            #assign place
                if row["Place"] != "":
                    my_graph.add((subj, place, Literal(str(row["Place"]))))
            
            #assign author
                if row["Author"] != "":
                    authors = object_mapping[row["Id"]]
                    for auth in authors:
                        my_graph.add((subj, relAuthor, auth))

        # Store RDF data in SPARQL endpoint
        #Utilizza una query SPARQL per contare direttamente il numero di triple nel database e memorizza il risultato in una variabile store_count.
        
            store = SPARQLUpdateStore()

            endpoint = self.getDbPathOrUrl() + "sparql" 

            store.open((endpoint, endpoint))  #first paramenter reading data, second write database

            for triple in my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()
            

        '''except Exception as e:
            print("Failed to upload data to Blazegraph: " + str(e))
            #traceback.print_exc()  # Stampa l'intero traceback per ottenere più informazioni sull'errore
            return False'''
        


grp_dbUrl = "http://192.168.1.163:9999/blazegraph/"
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_dbUrl)
metadata.pushDataToDb("../data/meta.csv")

# java -server -Xmx1g -jar blazegraph.jar (terminal command to run Blazegraph)



