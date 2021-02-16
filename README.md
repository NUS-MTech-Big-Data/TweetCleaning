# Tweets Cleaning
A scala project that
1. Consume Tweets from a Kafka topic (twitter.raw)
2. Perform some data cleaning
3. Push the message back to Kafka (twitter.clean)

## Prerequisite
1. Install sbt
We will use sdkman to install sbt
```
# uninstal sbt if already installed via brew
brew uninstall sbt

curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

sdk install sbt
```

2. Install Java 11
```
sdk list java
sdk install java 11.0.10.hs-adpt
```

3. Use Scala version 2.11.11

## Usage
```
sbt
run
```

