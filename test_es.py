from pyspark import SparkContext
import json

sc = SparkContext()
rdd = sc.parallelize([{'num': i} for i in range(10)])
def remove__id(doc):
    # `_id` field needs to be removed from the document
    # to be indexed, else configure this in `conf` while
    # calling the `saveAsNewAPIHadoopFile` API
    doc.pop('_id', '')
    return doc

new_rdd = rdd.map(remove__id).map(json.dumps).map(lambda x: ('key', x))

new_rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes" : 'localhost',
        "es.port" : '9200',
        "es.resource" : '%s/%s' % ('index_name', 'doc_type_name'),
        "es.input.json": 'true'
    }
)

print("ok test")