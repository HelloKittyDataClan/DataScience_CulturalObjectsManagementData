import pandas as pd
from pandas import Series
import json
from typing import Any, Dict, List
from sqlite3 import connect

# Define the data type for lists of dictionaries
DataType = List[Dict[str, Any]]

class Handler:  # Chiara
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

_____________________RELATIONAL DATA BASE____________________________

class ProcessDataUploadHandler(UploadHandler):  #Cata
    def __init__(self):
        super().__init__()

    #Create data frame with the objects ID beacuse activities refers to. (Data Frame is a function that allows us to create kind of tables with pandas)
    def pushDataToDbObject(self, file_path: str) -> pd.DataFrame:
        cultural_objects = pd.read_csv(file_path, keep_default_na=False,
                            dtype={
                                "Id": "string", 
                                "Type": "string", 
                                "Title": "string",
                                "Date": "string", 
                                "Author": "string", 
                                "Owner": "string",
                                "Place": "string"})
        
        objects_ids = cultural_objects[["Id"]]
        objects_internal_ids = ["object-" + str(idx) for idx in range(len(objects_ids))]
        objects_ids.insert(0, "objectId", pd.Series(objects_internal_ids, dtype="string"))
        
        objects_ids_df = pd.DataFrame(objects_ids)
        objects_ids_df.to_csv('objects_ids.csv', index=False)
        
        return objects_ids_df

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
    
    # Function to join activities with objects based on object id (to guarantee that the activities are connected with the objects)
    def joinActivitiesWithObjects(self, df_activities: pd.DataFrame, objects_ids_df: pd.DataFrame, left_on: str, right_on: str) -> pd.DataFrame:
        return pd.merge(df_activities, objects_ids_df, left_on=left_on, right_on=right_on, how="left")

    #Replace object id with objectId (internal id of objects). Two cases with the row technique included (activities) or without this data
    def extractAndRenameColumns(self, df: pd.DataFrame, include_technique: bool = False) -> pd.DataFrame:
        columns = ["internalId", "object id", "responsible institute", "responsible person", "tool", "start date", "end date"]
        if include_technique:
            columns.insert(4, "technique")  # Insert 'technique' column before 'tool'
        
        identifiers = df[columns]
        identifiers = identifiers.rename(columns={"object id": "objectId"})
        return identifiers
        
    #Create individual DataFrame tables calling the pushDataToDbActivities, internal ID, etc.
    def createTablesActivity(self, activities_file_path: str, objects_file_path: str):
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

        #Load object IDs
        objects_ids_df = self.pushDataToDbObject(objects_file_path)

        #Join activity DataFrames with objects DataFrame
        acquisition_joined = self.joinActivitiesWithObjects(acquisition_df, objects_ids_df, "object id", "objectId")
        processing_joined = self.joinActivitiesWithObjects(processing_df, objects_ids_df, "object id", "objectId")
        modelling_joined = self.joinActivitiesWithObjects(modelling_df, objects_ids_df, "object id", "objectId")
        optimising_joined = self.joinActivitiesWithObjects(optimising_df, objects_ids_df, "object id", "objectId")
        exporting_joined = self.joinActivitiesWithObjects(exporting_df, objects_ids_df, "object id", "objectId")

        #Extract and rename columns, including 'technique' for acquisition
        acquisition_final_db = self.extractAndRenameColumns(acquisition_joined, include_technique=True)
        processing_final_db = self.extractAndRenameColumns(processing_joined)
        modelling_final_db = self.extractAndRenameColumns(modelling_joined)
        optimising_final_db = self.extractAndRenameColumns(optimising_joined)
        exporting_final_db = self.extractAndRenameColumns(exporting_joined)
        
        #Save to SQLite database in the file
        with connect("activities.db") as con:
            objects_ids_df.to_sql("ObjectId", con, if_exists="replace", index=False)
            acquisition_final_db.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_final_db.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_final_db.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_final_db.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_final_db.to_sql("Exporting", con, if_exists="replace", index=False)
        
        #Printing the tables in the console (not necessary)
        print("\nAcquisition DB:")
        print(acquisition_final_db)
        print("\nProcessing DB:")
        print(processing_final_db)
        print("\nModelling DB:")
        print(modelling_final_db)
        print("\nOptimising DB:")
        print(optimising_final_db)
        print("\nExporting DB:")
        print(exporting_final_db)

#How to implement the code (example):
process_upload = ProcessDataUploadHandler()
process_upload.createTablesActivity('process.json', 'meta.csv')


_____________QUERIES_____________________________

class QueryHandler(Handler): #Elena
    def __init__(self):
        super().__init__()

    def getById(self, ID: str):
        try:
            with connect(self.getDbPathOrUrl()) as con:
                query = f"SELECT * FROM ObjectId WHERE Id = '{ID}'"
                result = pd.read_sql(query, con)
                return result
        except Exception as e:
            print(f"An error occurred: {e}")

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllActivities(self):
        try:
            # Modify the partialName parameter to include "wildcard characters" for partial matching
            # nell'input se inserisco anche solo un risultato parziale mi compare comunque
            with connect(self.getDbPathOrUrl()) as con:
                # Connect to the database #modific con la parte di catalina
                query = """
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, technique
                    FROM acquisition
                    UNION 
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS objectId, NULL AS technique
                    FROM processing
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", NULL AS objectId, NULL AS technique
                    FROM modelling
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM optimising
                    UNION
                    SELECT InternalId, "responsible institute", "responsible person", tool, "start date", "end date", objectId, NULL AS technique
                    FROM exporting
                """
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
                query2_table = pd.read_sql(query2, con, params=[partial_name]*5)
                return query2_table
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

        
__________________________ TESTS ___________________________

#Calling all the functions:
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("activities.db")

#Calling to get all activities
all_activities = process_query.getAllActivities()
print("All Activities:")
print(all_activities)

#Calling to get activities by responsible institution
partial_name_institution = "Council"
activities_by_institution = process_query.getActivitiesByResponsibleInstitution(partial_name_institution)
print(f"Activities by institution containing '{partial_name_institution}':")
print(activities_by_institution)

#Calling to get activities by a responsible person
partial_name_person = "Jane Doe"
activities_by_person = process_query.getActivitiesByResponsiblePerson(partial_name_person)
print(f"Activities by responsible person '{partial_name_person}':")
print(activities_by_person)

#Calling to get activities by tool
partial_name_tool = "Nikon"
activities_by_tool = process_query.getActivitiesUsingTool(partial_name_tool)
print(f"Activities by responsible tool '{partial_name_tool}':")
print(activities_by_tool)

#Calling to get activities started after a specific date
start_date = "2023-04-17"
activities_started_after = process_query.getActivitiesStartedAfter(start_date)
print(f"Activities started after '{start_date}':")
print(activities_started_after)

#Calling to get activities ended after a specific date
end_date = "2023-03-04"
activities_end_after = process_query.getActivitiesEndedBefore(end_date)
print(f"Activities ended after '{end_date}':")
print(activities_end_after)

#Calling to get activities by technique
technique_partial_name = "Structured-light 3D scanner"
acquisitions_by_technique = process_query.getAcquisitionsByTechnique(technique_partial_name)
print(f"Acquisitions with technique containing '{technique_partial_name}':")
print(acquisitions_by_technique)

