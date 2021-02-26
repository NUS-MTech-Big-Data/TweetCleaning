# Tweets Cleaning
A scala project that
1. Consume Tweets from a Kafka topic (twitter.raw)
2. Perform some data cleaning
3. Push the message back to Kafka (twitter.clean)

## Prerequisite
1. Install sbt  
We will use sdkman to install sbt
```
# Uninstal sbt if already installed via brew
brew uninstall sbt

# Install sdkman
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

sdk install sbt
```

2. Install Java 8 
```
sdk list java
sdk install java 8.0.282.hs-adpt
```

3. Install Spark 2.4.7
```
sdk list spark
sdk install spark 2.4.7
```

## Usage
```
sbt
run
```

