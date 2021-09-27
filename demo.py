import apache_beam as beam

def filtering(record):
    return record[4] == 'NJ'

p = beam.Pipeline()
lines = p | beam.io.ReadFromText('data/addresses.csv')
splited = lines | beam.Map(lambda element: element.split(','))
filtered = splited | beam.Filter(filtering)
formated = filtered | beam.Map(lambda record: (record[0],1))
counted = formated | beam.CombinePerKey(sum)
counted | beam.Map(print)

p.run()