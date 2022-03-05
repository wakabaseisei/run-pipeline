from email.policy import default
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import TaggedOutput
import json
from apache_beam import window


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_subscription',
            help='Input path for the pipeline')
        parser.add_argument(
            "--window_size",
            type=float,
            default=1.0,
            help="Output file's window size in minutes.",
        )
        parser.add_argument(
            "--output_path",
            help="Path of the output GCS file including the prefix.",
        )


class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'

    def process(self, message):
        try:
            data = json.loads(message)
            yield data
        except Exception as error:
            error_row = json.loads(message)
            yield TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)


class WriteToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch, window=beam.DoFn.WindowParam):
        ts_format = "%Y%m%d%H%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        filename = "-".join([self.output_path, window_start, window_end])

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write("{}\n".format(json.dumps(element)).encode("utf-8"))


def is_intj(record):
    return record["type"] == "INTJ"


def run():
    options = MyOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=options)

    (p
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=options.input_subscription, with_attributes=True)
     | 'ConfigrationWindow' >> beam.WindowInto(window.FixedWindows(int(options.window_size)))
     | 'ParseFromJson' >> beam.ParDo(ParseMessage())
     | 'FilterMBTIType' >> beam.Filter(is_intj)
     | 'WriteToGCS' >> beam.ParDo(WriteToGCS(options.output_path))
     )

    p.run().wait_until_finish()


if __name__ == '__main__':

    run()
