def is_float(element):
    try:
        float(element)
    except ValueError:
        return False
    else:
        return True


csv = sc.textFile('/home/ubuntu/Meteorite_Landings.csv')  # Read csv

meteorites = csv.map(lambda line: line.split(','))  # Split by fileds

coordinates_raw = meteorites.map(lambda meteorite: (meteorite[7], meteorite[8]))  # Get coordinates fields

coordinates_clean = coordinates_raw.filter(lambda coordinates: is_float(coordinates[0]) and float(coordinates[0]) != 0.0 and is_float(coordinates[1]) and float(coordinates[1]) != 0.0)  # Filter useless data

meteorites_regions = coordinates_clean.map(lambda coordinates: ((int(float(coordinates[0])), int(float(coordinates[1]))), 1))  # map: assign 1 value

meteorites_by_region = meteorites_regions.reduceByKey(lambda x, y: x+y)  # reduce: acum value

meteorites_by_region.takeOrdered(1, key=lambda x: -x[1])  # Get win location!
