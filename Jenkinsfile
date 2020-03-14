#!/usr/bin/env groovy

pipeline {
    agent {
        label 'os:linux'
    }

    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(
            daysToKeepStr: '7'
        ))
    }

    environment {
        RETENTION_CHECKOUT_DIR = "${env.WORKSPACE}/retention"
        RETENTION_ARCHIVE_DIR = "${env.WORKSPACE}/retention-archive"
    }

    stages {
        stage('Clean Libs') {
            steps {
                cleanWs()
                sh """
                    mkdir -p  ${env.RETENTION_ARCHIVE_DIR}
                """
            }
        }

        stage('Checkout') {
            steps {
                dir(env.RETENTION_CHECKOUT_DIR) {
                    checkout scm
                }
            }
        }

        stage('Install Requirements') {
            steps {
                dir(env.RETENTION_CHECKOUT_DIR) {
                    sh """
                        pip3 install -r requirements.txt -t ${env.RETENTION_ARCHIVE_DIR}
                        cp -r ${env.RETENTION_CHECKOUT_DIR}/* ${env.RETENTION_ARCHIVE_DIR}
                    """
                }
            }
        }

        stage('Package') {
            steps {
                script {
                    def artifactName = artifactName(name: 'retention', extension: "tar.gz")
                    dir(env.RETENTION_ARCHIVE_DIR) {
                        sh "tar czf ${env.WORKSPACE}/${artifactName} *"
                    }
                }
            }
        }

        stage('Archive') {
            steps {
                archiveArtifacts artifacts: '*.tar.gz', onlyIfSuccessful: true
            }
        }
    }
}