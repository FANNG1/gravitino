./gradlew :maintenance:optimizer:spotlessApply
#./gradlew :optimizer:build -x test
./gradlew clean compileDistribution -x test -x rat  -x lintOpenAPI -x web
