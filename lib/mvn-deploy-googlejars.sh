#1/bin/bash

# Assumes executing in {ROOT_DIR}/lib
# Assumes Simba JDBC jars for GBQ have been placed in {ROOT_DIR}/lib/bigquery_jars/


mvn deploy:deploy-file -Dfile=./bigquery_jars/GoogleBigQueryJDBC42.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=GoogleBigQueryJDBC -Dversion=42 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;
                       
mvn deploy:deploy-file -Dfile=./bigquery_jars/google-api-client-1.28.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-api-client \
                       -Dversion=1.28.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-http-client-1.29.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-http-client \
                       -Dversion=1.29.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/gax-1.42.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=gax \
                       -Dversion=1.42.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-http-client-jackson2-1.28.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-http-client-jackson2 \
                       -Dversion=1.28.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-oauth-client-1.28.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-oauth-client \
                       -Dversion=1.28.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-auth-library-oauth2-http-0.13.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-auth-library-oauth2-http \
                       -Dversion=0.13.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-auth-library-credentials-0.15.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-auth-library-credentials \
                       -Dversion=0.15.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/jackson-core-2.9.6.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=jackson-core \
                       -Dversion=2.9.6 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/guava-26.0-android.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=guava  \
                       -Dversion=26.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/google-api-services-bigquery-V2-rev426-1.25.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=google-api-service-bigquery \
                       -Dversion=V2-rev426-1.25.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/opencensus-api-0.18.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=opencensus-api \
                       -Dversion=0.18.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/opencensus-contrib-http-util-0.18.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=opencensus-contrib-http-util \
                       -Dversion=0.18.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/grpc-context-1.18.0.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=grpc-context \
                       -Dversion=1.18.0 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/joda-time-2.10.1.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=joda-time \
                       -Dversion=2.10.1 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;

mvn deploy:deploy-file -Dfile=./bigquery_jars/avro-1.8.2.jar \
                       -DgroupId=com.simba.googlebigquery.jdbc \
                       -DartifactId=avro \
                       -Dversion=1.8.2 -Dpackaging=jar \
                       -DrepositoryId=local-maven-repo -DupdateReleaseInfo=true \
                       -Durl=file:./;
