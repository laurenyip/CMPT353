import sys
import time
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit relative scores').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=comments_schema)

    # TODO
    #Calculate the average score for each subreddit, as before.
    avg = comments.groupBy('subreddit').agg(functions.avg('score'))
    #Exclude any subreddits with average score ≤0.
    avg = avg.filter(avg['avg(score)'] > 0)
    avg = avg.cache()

    # Broadcast the DataFrame containing average scores
    #broadcast_avg = spark.sparkContext.broadcast(avg)
    
    
    #Join the average score to the collection of all comments. Divide to get the relative score.
    with_cmt = comments.join(avg, on='subreddit')
    #with_cmt = comments.join(broadcast_avg.value, on='subreddit')
    with_cmt = with_cmt.withColumn('rel_score', with_cmt['score'] / with_cmt['avg(score)'])
    
    #Determine the max relative score for each subreddit.
    
    max_relative_scores = with_cmt.groupBy('subreddit').agg(functions.max('rel_score').alias('rel_score'))
    # max_rel_scores.cache()
    
    #Join again to get the best comment on each subreddit: we need this step to get the author.
    bestAuthor = with_cmt.join(max_relative_scores, ['subreddit', 'rel_score'])
    bestAuthor = bestAuthor.select('subreddit', 'author', 'rel_score')
    
    #bestAuthor.write.json(out_directory, mode='overwrite')

    bestAuthor.coalesce(1).write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    

    main(in_directory, out_directory)


    # spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=8 reddit_relative.py reddit-3 output

    # time spark-submit reddit_relative.py reddit-3 output_directory  --conf spark.sql.autoBroadcastJoinThreshold=-1