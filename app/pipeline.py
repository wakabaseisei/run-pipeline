import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='./input.txt',
            help='Input path for the pipeline')

        parser.add_argument(
            '--output',
            default='./output.txt',
            help='Output path for the pipeline')


class ComputeWordLength(beam.DoFn):

    def __init__(self):
        pass

    def process(self, element):
        yield len(element)


def run():
    options = MyOptions()

    p = beam.Pipeline(options=options)

    (p
     | 'ReadFromText' >> beam.io.ReadFromText(options.input)
     | 'ComputeWordLength' >> beam.ParDo(ComputeWordLength())
     | 'WriteToText' >> beam.io.WriteToText(options.output))

    p.run()


if __name__ == '__main__':
    run()
