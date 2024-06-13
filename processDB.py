import pandas as pd
import json
from sqlite3 import connect

class UploadHandler:
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass
    
class ProcessDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    # Function to load objects data and generate internal IDs
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
        
        return objects_ids

    # Function to create a general DataFrame for all activities and save to CSV
    def pushDataToDbActivities(self, file_path: str) -> pd.DataFrame:
        with open(file_path, 'r') as file:
            activities = json.load(file)
        
        activities_table = []
        
        for item in activities:
            object_id = item["object id"]
            for activity_type, activity_data in item.items():
                if activity_type != "object id":
                    activity_data_flat = {
                        "object id": object_id, 
                        "activity type": activity_type
                    }
                    activity_data_flat.update(activity_data)
                    activities_table.append(activity_data_flat)
        
        activities_df = pd.DataFrame(activities_table)
        
        # Convert lists to strings if 'tool' column exists 
        #Tool is a list we need to convert it in string, using lamda function (not defined small function that contains a single expression)
        if 'tool' in activities_df.columns:
            activities_df['tool'] = activities_df['tool'].apply(lambda x: ', '.join(x))
            
        activities_df.to_csv('general_activities.csv', index=False)
                
        return activities_df

    # Function to generate internal IDs for general table of activities
    def activitiesInternalIds(self, df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        internal_ids = ["activity-" + str(idx) for idx in range(len(df))]
        df.insert(0, "InternalId", pd.Series(internal_ids, dtype="string"))
        return df

    # Function to query specific type of activities ex. acquisition
    def queryActivities(self, df: pd.DataFrame, activity_type: str) -> pd.DataFrame:
        return df[df["activity type"] == activity_type]

    # Function to join activities with objects based on object id
    def joinActivitiesWithObjects(self, activities_df: pd.DataFrame, objects_ids: pd.DataFrame, left_on: str, right_on: str) -> pd.DataFrame:
        return pd.merge(activities_df, objects_ids, left_on=left_on, right_on=right_on, how="left")
    

#TESTS __________________________________________________
if __name__ == "__main__": #construct in Python is used to allow or prevent parts of code from being run when the module is imported
    
    # Create an instance of ProcessDataUploadHandler
    handler = ProcessDataUploadHandler()

    # Load and process objects data
    objects_ids = handler.pushDataToDbObject("meta.csv")
    print("Objects with internal IDs:")
    print(objects_ids)

    #Load and process activities data
    general_activities = handler.pushDataToDbActivities("process.json")
    print("\nGeneral Table of Activities:")
    print(general_activities)

    #Generate internal IDs for general activities
    general_activities_ids = handler.activitiesInternalIds(general_activities, "object id")
    print("\nGeneral Activities with Internal IDs:")
    print(general_activities_ids)

    #Example of querying acquisition activities
    acquisition = handler.queryActivities(general_activities, "acquisition")
    print("\nAcquisition Activities:")
    print(acquisition)
    
    #Example of querying acquisition processing
    processing = handler.queryActivities(general_activities, "processing")
    print("\nProcessing Activities:")
    print(processing)
    
    #Example of querying acquisition modelling
    modelling = handler.queryActivities(general_activities, "modelling")
    print("\nModelling Activities:")
    print(modelling)
    
    #Example of querying acquisition optimising
    optimising = handler.queryActivities(general_activities, "optimising")
    print("\nModelling Activities:")
    print(optimising)
    
    #Example of querying acquisition exporting
    exporting = handler.queryActivities(general_activities, "exporting")
    print("\nModelling Activities:")
    print( exporting)
    
    #Cosi confermo che le ricerche funzionano
    # Example of joining acquisition activities with objects
    df_joined_acquisition = handler.joinActivitiesWithObjects(acquisition, objects_ids, "object id", "Id")
    print("\nAcquisition Activities Joined with Objects:")
    print(df_joined_acquisition)

    # Rename columns and extract required data (example with acquisition)
    acquisition_name = df_joined_acquisition[["objectId", "tool"]].rename(columns={"objectId": "internalId"})
    print("\nAcquisition Activities with Renamed Columns:")
    print(acquisition_name)


    with connect("activities.db") as con:
        objects_ids.to_sql("ObjectId", con, if_exists="replace", index=False)
        acquisition.to_sql("Acquisition", con, if_exists="replace", index=False)
        processing.to_sql("Processing", con, if_exists="replace", index=False)
        modelling.to_sql("Modelling", con, if_exists="replace", index=False)
        optimising.to_sql("Optimising", con, if_exists="replace", index=False)
        exporting.to_sql("Exporting", con, if_exists="replace", index=False)
