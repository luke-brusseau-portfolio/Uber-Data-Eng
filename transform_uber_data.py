import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index
    
    datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime['pickup_hour'] = datetime['tpep_pickup_datetime'].dt.hour
    datetime['pickup_day'] = datetime['tpep_pickup_datetime'].dt.day
    datetime['pickup_month'] = datetime['tpep_pickup_datetime'].dt.month
    datetime['pickup_year'] = datetime['tpep_pickup_datetime'].dt.year
    datetime['pickup_weekday'] = datetime['tpep_pickup_datetime'].dt.weekday

    datetime['tpep_dropoff_datetime'] = datetime['tpep_dropoff_datetime']
    datetime['dropoff_hour'] = datetime['tpep_dropoff_datetime'].dt.hour
    datetime['dropoff_day'] = datetime['tpep_dropoff_datetime'].dt.day
    datetime['dropoff_month'] = datetime['tpep_dropoff_datetime'].dt.month
    datetime['dropoff_year'] = datetime['tpep_dropoff_datetime'].dt.year
    datetime['dropoff_weekday'] = datetime['tpep_dropoff_datetime'].dt.weekday

    datetime['datetime_id'] = datetime.index

    datetime = datetime[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
                                'tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month', 'dropoff_year', 'dropoff_weekday']]


    passenger_count = df[['passenger_count']].reset_index(drop=True)
    passenger_count['passenger_count_id'] = passenger_count.index
    passenger_count = passenger_count[['passenger_count_id','passenger_count']]

    trip_distance = df[['trip_distance']].reset_index(drop=True)
    trip_distance['trip_distance_id'] = trip_distance.index
    trip_distance = trip_distance[['trip_distance_id','trip_distance']]
    
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code = df[['RatecodeID']].reset_index(drop=True)
    rate_code['rate_code_id'] = rate_code .index
    rate_code['rate_code_name'] = rate_code['RatecodeID'].map(rate_code_type)
    rate_code = rate_code[['rate_code_id','RatecodeID','rate_code_name']]


    pickup_loc = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_loc['pickup_location_id'] = pickup_loc.index
    pickup_loc = pickup_loc[['pickup_location_id','pickup_latitude','pickup_longitude']] 


    dropoff_loc = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_loc['dropoff_location_id'] = dropoff_loc.index
    dropoff_loc = dropoff_loc[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type = df[['payment_type']].reset_index(drop=True)
    payment_type['payment_type_id'] = payment_type.index
    payment_type['payment_type_name'] = payment_type['payment_type'].map(payment_type_name)
    payment_type = payment_type[['payment_type_id','payment_type','payment_type_name']]

    fact_table = df.merge(passenger_count, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance, left_on='trip_id', right_on='trip_distance_id') \
             .merge(rate_code, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_loc, left_on='trip_id', right_on='pickup_location_id') \
             .merge(dropoff_loc, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime, left_on='trip_id', right_on='datetime_id') \
             .merge(payment_type, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]

    return {"datetime":datetime.to_dict(orient="dict"),
    "passenger_count":passenger_count.to_dict(orient="dict"),
    "trip_distance":trip_distance.to_dict(orient="dict"),
    "rate_code":rate_code.to_dict(orient="dict"),
    "pickup_location":pickup_location.to_dict(orient="dict"),
    "dropoff_location":dropoff_location.to_dict(orient="dict"),
    "payment_type":payment_type.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'