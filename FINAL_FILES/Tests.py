from pandas import DataFrame
from UML_handler import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from UML_mashup import BasicMashup, AdvancedMashup
from UML_classes import Person, CulturalHeritageObject, Activity, Acquisition
from UML_classes import IdentifiableEntity, Person, CulturalHeritageObject, Activity, Acquisition, Processing, Modelling, Optimising, Exporting, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map



#__________________________ TESTS QUERIES___________________________

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

 

#__________________________ TESTS BASIC MASHUP___________________________

mashup = BasicMashup()
process_query = ProcessDataQueryHandler()
process_query.setDbPathOrUrl("activities.db")

# Adding process query handler to mashup
mashup.addProcessHandler(process_query)

# Calling the method 1
all_activities = mashup.getAllActivities()
for i in all_activities:
    print(i.institute)

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
    print(activity)

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


 
#------------------TEST-----------------------------

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



#------------------TEST-----------------------------

if __name__ == "__main__":
    # Creazione di un'istanza di AdvancedMashup
    mashup = AdvancedMashup()

    # Creazione degli oggetti culturali con i dati forniti
    portrait_of_ulisse_aldrovandi = Painting("13", "Portrait of Ulisse Aldrovandi", "Accademia Carrara", "Bergamo", ["Carracci, Agostino (ULAN:500115349)"], "1582-1585")
    map_of_botanical_garden_bologna = Map("16", "Map of the botanical garden in Bologna", "Orto Botanico ed Herbarium di Bologna", "Bologna", ["Monti, Giuseppe (VIAF:54929912)"], "1753")
    
    # Creazione delle attività associate alle istituzioni responsabili
    activity1 = Activity("Council", "Alice Liddell", {"Nikon D7200 Nikor 50mm"}, "2023-03-24", "2023-03-24", portrait_of_ulisse_aldrovandi)
    activity2 = Activity("Council", "Alice Liddell", {"3DF Zephyr"}, "2023-03-28", "2023-03-29", portrait_of_ulisse_aldrovandi)

    # Aggiunta delle attività all'istanza di AdvancedMashup
    mashup.activities.append(activity1)
    mashup.activities.append(activity2)
    
    # Utilizzo del metodo getObjectsHandledByResponsibleInstitution
    institution_name = "Council"  # Istituzione responsabile da cercare
    matched_objects = mashup.getObjectsHandledByResponsibleInstitution(institution_name)
    
    # Stampa dei risultati
    print(f"Oggetti culturali gestiti dall'istituzione che contiene '{institution_name}':")
    for obj in matched_objects:
        if isinstance(obj, Painting):
            print(f"- {obj.getTitle()} (Tipo: Painting, Proprietario: {obj.getOwner()}, Luogo: {obj.getPlace()}, Autori: {', '.join(obj.getAuthors())}, Data: {obj.getDate()})")
        elif isinstance(obj, Map):
            print(f"- {obj.getTitle()} (Tipo: Map, Proprietario: {obj.getOwner()}, Luogo: {obj.getPlace()}, Autori: {', '.join(obj.getAuthors())}, Data: {obj.getDate()})")
        else:
            print(f"- {obj.getTitle()} (Tipo: {type(obj).__name__})")
