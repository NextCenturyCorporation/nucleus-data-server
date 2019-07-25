pipeline {
  agent none
  
  environment {
    PATH = "/usr/local/bin:/usr/bin:/usr/sbin:/bin:/sbin"
    HOME = "."
  }

  options {
    timestamps()
  }

  stages {
    stage('Build') {
      agent {
        docker {
          image 'adoptopenjdk/openjdk10:alpine'
          args '--network=host'
        }
      }
      steps {
        sh './gradlew -x test clean build'
      }
    }

    stage('Unit Test') {
      agent {
        docker {
          image 'adoptopenjdk/openjdk10:alpine'
          args '--network=host'
        }
      }
      steps {
        sh './gradlew test'
        junit testResults: '**/build/test-results/**/*.xml',  keepLongStdio: true, allowEmptyResults: false
      }
    }

    stage('Prepare Container') {
      agent {
        docker {
          image 'adoptopenjdk/openjdk10:alpine'
          args '--network=host'
        }
      }
      steps {
        sh './gradlew -x test clean dockerCopy'
        stash includes: 'build/docker', name: 'docker' 
      }
    }


    stage('Publish Container') {
      agent any
      when {
          anyOf { branch 'master'; branch 'THOR-jenkins-pipeline'; }
      }
      steps {
        withCredentials([usernamePassword(
          credentialsId: 'aws_jenkins',
          usernameVariable: 'AWS_ACCESS_KEY_ID',
          passwordVariable: 'AWS_SECRET_ACCESS_KEY'                              
        )]) {
          sh 'mkdir -p build/docker'
          sh 'chmod -R u+w build/docker'
          unstash 'docker';

          sh 'rm  ~/.dockercfg || true'
          sh 'rm ~/.docker/config.json || true'
          sh 'pip3 install awscli --user'

          dir('build/docker') {
            script {
              //configure registry
              sh("eval \$(aws ecr get-login --no-include-email --region us-east-1 | sed 's|https://||')")

              docker.withRegistry('https://670848316581.dkr.ecr.us-east-1.amazonaws.com') {
                  //build image
                  def customImage = docker.build("neon/server:${env.BUILD_ID}")
                   
                  //push image
                  customImage.push()
              }        
            } 
          }
        }
      }
    } 
  }
}