# 콘솔에서 진행
# ./bin/pyspark | pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

spark = SparkSession.builder.appName("test-spark").master("local[*]").getOrCreate()
textFile = spark.read.text('README.md')
textFile.count()
textFile.first()

linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
linesWithSpark.count()

# 가장 긴 줄을 탐색
# 아직 모든게 어떤걸 하는지 모르지만 유추해보자면 아래와 같다
# textFile 을 줄바꿈(\s+) 로 split -> 해당 줄 size(length) 측정) -> numWords 로 저장 -> numWords 에서 max 찾기 ->
# maxWords 로 이름 변경 -> collect (실행?)
# 대략적으로 맞다
textFile.select(sf.size(sf.split(textFile.value, "\s+")).
                name('numWords')).agg(sf.max('numWords').alias('maxWords')).collect()

# 각 줄별로 단어 사용 횟수 구하기
# explode는 줄로 되어 있는 데이터셋을 단어로 된 데이터셋으로 변환
# 단어들은 words 라는 이름으로 만들어지고
# groupby 해서 count 수 구함
wordCounts = textFile.select(sf.explode(sf.split(textFile.value, "\s+")).alias('words')).groupBy('words').count()
wordCounts.orderBy('count', ascending=False).collect()
