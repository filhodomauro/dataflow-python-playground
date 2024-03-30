import re
import argparse
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

logger = logging.getLogger(__name__)

class WordsExtractDoFn(beam.DoFn):
    
    def process(self, element):
       return re.findall(r'[\w\']+', element, re.UNICODE)

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results'
    )
    known_args, extra_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(extra_args)

    with beam.Pipeline(options=pipeline_options) as p:
        output = (p 
                  | 'Ready Text' >> ReadFromText(known_args.input)
                  | 'Extract' >> beam.ParDo(WordsExtractDoFn()).with_output_types(str)
                  | "MapValue" >> beam.Map(lambda x: (x,1))
                  | 'Aggreagate' >> beam.CombinePerKey(sum)
                  | 'Write' >> WriteToText(known_args.output)
                  )

if __name__ == '__main__':
  run()