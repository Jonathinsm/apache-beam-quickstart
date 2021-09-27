import apache_beam as beam

p = beam.Pipeline()

count = {
    p
    | beam.io.ReadFromText('data/addresses.csv')
    | beam.Map(lambda element: element.split(','))
    | beam.Map(print)
}

p.run()