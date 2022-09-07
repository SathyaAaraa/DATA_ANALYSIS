#Import required packages
from imports import *
from utilities import *

#Initialise Flask object
app = Flask(__name__)

class TripDataAnalysisV1():
    def __init__(self):
        '''
        Initialise and read all configuration parameters
        '''
        config = ConfigParser()
        config.read('../config/trip_data_analysis_config.ini')
        self.connection_string = config.get('mongo-db-information', 'connection_string')
        self.mongo_query_path = config.get('query-pipeline', 'mongo_query_path')

    def analyse_trip_data(self, DBName, CollectionName):
        '''
        In this method we analyse trip data stored in Mongo DB and retrieve below information
        1) Number of trips whose total_amount is less than 5dollar
        2) Find hour at which maximum trip has been made
        3) Average of tip amount
        :return: dict of above mentioned 3 requirements
        '''

        #Create a Mongo DB client
        client = MongoClient(self.connection_string)

        #Initialise an object for given Database and collection
        trip_collection = client[DBName][CollectionName]

        '''mongo_query_path is a json file path. This file contains queries and pipelines 
        used to pull required information from Mongo DB.
        mongo_query is a dictionary of queries'''
        mongo_query = load_json(self.mongo_query_path)

        #Reading queries from mongo_query dict and store it in variable
        number_of_trips_less_than_5dollar_query = mongo_query['number_of_trips_less_than_5dollar']
        trip_amount_per_hour_pipeline = mongo_query['trip_amount_per_hour_pipeline']
        average_tip_amount_pipeline = mongo_query['average_tip_amount_pipeline']

        #Get number of documents with total_amount less than 5 dollar
        number_of_trips_less_than_5dollar = trip_collection.count_documents(number_of_trips_less_than_5dollar_query)

        #Get sum of total amount per hour - Here we groupby hour and take sum of total_amount field
        trip_amount_per_hour_pipeline_cursor = trip_collection.aggregate(trip_amount_per_hour_pipeline)
        trip_amount_per_hour_pipeline_list = []

        #Iterate cursor to retrieve queried values
        for each_doc in trip_amount_per_hour_pipeline_cursor:
            trip_amount_per_hour_pipeline_list.append(each_doc)

        #create a dataframe on queried values and filter out hour at which maximum trip amount has been made.
        trip_amount_per_hour_pipeline_temp_df = pd.DataFrame(trip_amount_per_hour_pipeline_list)
        trip_amount_per_hour_pipeline_temp_df.rename(columns = {'_id':'hour'}, inplace = True)
        trip_amount_per_hour_pipeline_max_hour = trip_amount_per_hour_pipeline_temp_df[trip_amount_per_hour_pipeline_temp_df['trip_amount_per_hour']==trip_amount_per_hour_pipeline_temp_df['trip_amount_per_hour'].max()].reset_index()['hour'][0]

        #Get average tip amount
        average_tip_amount_cursor = trip_collection.aggregate(average_tip_amount_pipeline)

        #Iterate over cursor to retrieve queried value
        for each_doc in average_tip_amount_cursor:
            average_tip_amount = each_doc['average_tip_amount']

        #Return dictionary
        return {'Number of Trips paid less than 5 dollar' : number_of_trips_less_than_5dollar,
                'average tip amount' : average_tip_amount,
                'Hour at which Max Trip amount has been paid':  "At " + str(trip_amount_per_hour_pipeline_max_hour) + "th hour maximum amount has been paid and total amount is " + str( trip_amount_per_hour_pipeline_temp_df['trip_amount_per_hour'].max() ),
                }


@app.route('/trip_data_analysis', methods=["POST"])
def trip_data_analysis():

    #Read request and parse key-value pairs
    request_data = request.json
    DBName = request_data.get("DB_NAME", None)
    CollectionName = request_data.get("COLLECTION_NAME", None)

    #Log Start Timestamp
    StartTimeStamp = datetime.now()
    print("StartTimeStamp -> ", StartTimeStamp)

    # Create an object for testDataGeneratorV1 Class
    TripDataAnalysis = TripDataAnalysisV1()

    response = TripDataAnalysis.analyse_trip_data(DBName, CollectionName)

    #Log End Timestamp
    EndTimeStamp = datetime.now()
    print("EndTimestamp -> ", EndTimeStamp)

    return response

if __name__ == '__main__':
   app.run(debug=True, host="127.0.0.1", port=5051)
