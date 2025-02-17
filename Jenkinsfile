#!/usr/bin/env groovy


def java = null
def maven = null
def mavenEnv = null
def version = null
def mvnLocalHome = "/opt/m2repos/pulsar-io-google-bigtable"
def img = null

/***
 * A manual build will have UserIdCause. Triggered builds have different causes.
 ***/
def wasManuallyKickedOff() {
  return currentBuild.rawBuild.getCauses()[0].class.toString().endsWith('UserIdCause')
}

/***
 * We only release from master. Also, we do not want to push on every master build, but only those
 * that are manually-initiated. This will prevent failed builds due to Jenkins trying to push artifacts
 * for previously-released versions when builds are triggered by things like branch indexing.
 ***/
def isRelease() {
  return env.BRANCH_NAME == 'master' && wasManuallyKickedOff()
}

def currentVersion(mavenEnv) {
  withEnv(mavenEnv) {
    def pom = readMavenPom file: 'pom.xml'
    def version = sh script: "echo ${pom.getVersion().trim()} | grep -Eo '[0-9\\.]+'", returnStdout: true
    return version.trim()
  }
}

def createVersion(version) {
  version = version + (isRelease() ? '' : "-${env.BRANCH_NAME}-SNAPSHOT")
  return version
}

node('docker') {
  stage('Initialize') {
    checkout scm

    java = tool "Java 8"
    maven = tool "maven 3.3.9"
    mavenEnv = ["MVN_HOME=${maven}", "JAVA_HOME=${java}", "PATH+MAVEN=${maven}/bin"]
    version = createVersion(currentVersion(mavenEnv))
  }

  stage('Build') {
    withEnv(mavenEnv) {
      sh "mvn clean install -B -U -e -Dmaven.repo.local=${mvnLocalHome} "
    }
  }


  stage('Push') {
    withEnv(mavenEnv) {
      if (isRelease()) {
        sh "mvn -Dresume=false release:clean release:prepare release:perform -Dmaven.javadoc.skip=true -DskipTests -Dmaven.repo.local=${mvnLocalHome}"
      }
      else {
        sh "mvn versions:set -DnewVersion=${version} -Dmaven.repo.local=${mvnLocalHome}"
        sh "mvn deploy -DskipTests -Dmaven.repo.local=${mvnLocalHome}"
      }
    }
  }
}
