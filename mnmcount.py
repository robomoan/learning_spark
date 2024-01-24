import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # SparkSession 객체 생성
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
    # 실행시 콘솔 출력 메시지 레벨 WARN으로 조정(기본값은 INFO)
    spark.sparkContext.setLogLevel('WARN')

    # 명령줄에서 M&M 데이터셋 파일 얻기
    mnm_file = sys.argv[1]
    # 얻은 CSV 파일을 DataFrame 형식으로 변환해 mnm_df 변수에 할당
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    mnm_df.show(n=5, truncate=False)

    # 주별, 색깔별 개수 합계 구하기, 합계 내림차순으로 정렬
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # 집계 결과 상위 10개 행 출력 및 전체 행 개수 출력
    count_mnm_df.show(n=10, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # 캘리포니아 주 색깔별 합계 구하기, 합계 내림차순으로 정렬
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # 집계 결과 출력
    ca_count_mnm_df.show(n=10, truncate=False)

    # SparkSession 종료
    spark.stop()