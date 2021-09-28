import apache_beam as beam

def filtering(element):
    return element[4] == 'NJ'

p = beam.Pipeline()

lines = p | beam.io.ReadFromText('data/addresses.csv')
splited = lines | beam.Map(lambda linea: linea.split(','))
filtrado = splited | beam.Filter(filtering)
formated = filtrado | beam.Map(lambda record: (record[0],1))
counted = formated | beam.CombinePerKey(sum)
counted | beam.Map(print)

p.run()
