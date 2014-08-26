export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home
PERFORMANCE="-Xmx4G"
OPTIONS="-Djava.awt.headless=true"
java $PERFORMANCE $OPTIONS -cp "target/classes:rea-deps.jar" "$@"
