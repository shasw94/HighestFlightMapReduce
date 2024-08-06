#!/usr/bin/env python
# coding: utf-8

import multiprocessing as mp
import csv

def map(r):
    '''
        Change row to (passenger id, 1) format or (key, value)
        @param r: each row in data
        @return tuple (key, value)
    '''
    # If length of row is not 6 columns dont proceed forward
    if len(r) != 6:
        return
    try:
        # strip of spaces and change cases to upper for treating variations in these as same key
        return (r[0].strip().upper(), 1)
    except ValueError:
        return


def shuffle(mapper_out): 
    """ 
        Organise the mapped values by key
        @param mapper_out array of (key, value) tuples
        @return data dictionary of key as passenger id and values as grouped data of mapper_out
    """ 
    data = {} 
    for k, v in filter(None, mapper_out): 
        if k not in data: 
            data[k] = [v] 
        else: 
            data[k].append(v) 
    return data


def reduce(kv):
    '''
        Sum of values in shuffled dictionary
        @param kv: dictionary {passenger_id, values} values are the list of 1s
        @return sum of 1s or values of passenger, signifying total number of flights
    '''
    k, v = kv
    return k, sum(v)


if __name__ == '__main__':
    map_in = []
    # open the CSV file
    with open(r'./AComp_Passenger_data_no_error.csv', 'r', encoding='utf-8') as file:
        # Read rows as an array
        map_in = list(csv.reader(file, delimiter=','))
        with mp.Pool(processes=mp.cpu_count()) as pool:
            # Change to key value pair (passenger_id, 1)
            map_out = pool.map(map, map_in)
            # Shuffle the key value pairs so that all the 1(s) of the same passenger are together [{passenger_id: [1,1,1,...]}]
            reduce_in = shuffle(map_out)
            # reduce or sum of the shuffled data, as we are counting the total number of flights. 
            reduce_out = pool.map(reduce, reduce_in.items())            

    # Highest flight number
    highFlightNumber = max(reduce_out, key= lambda x : x[1])[1]

    # Finding individual(s) with the highest flight number
    listHihFlighNumbers = list(filter(lambda arr : arr[1] == highFlightNumber, reduce_out))
    print("Passengers with highest number of flights are:")
    [print(f'Passenger ID: {passenger} | Number of hours: {hours}') for passenger, hours in listHihFlighNumbers]
