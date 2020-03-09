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

    parameters {
        booleanParam(name: 'AUTO_DEPLOY',
            defaultValue: true,
            description: 'When checked, will automatically deploy to dev environment.')
    }

    environment {
        RETENTION_BUILD_SCRIPT_DIR = "${env.WORKSPACE}/retention"
        RETENTION_ARCHIVE_DIR = "${env.WORKSPACE}/retention-archive"
    }

    stages {
        stage('Clean Libs') {
            steps {
                 sh """
                    rm -rf ${env.RETENTION_ARCHIVE_DIR}
                    mkdir -p  ${env.RETENTION_ARCHIVE_DIR}
                """
            }
        }

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Package') {
            steps {
                dir(env.RETENTION_BUILD_SCRIPT_DIR) {
                    sh """
                        pip3 install -r requirements.txt -t ${env.RETENTION_ARCHIVE_DIR}
                        cp -r ${env.RETENTION_BUILD_SCRIPT_DIR}/* ${env.RETENTION_ARCHIVE_DIR}
                    """
                }
            }
        }
        
        stage('Archive') {
            steps {
                def artifactName = artifactName(name: 'rentention', extension: "tar.gz")
                sh "tar czf ${env.WORKSPACE}/${artifactName} ${env.RETENTION_ARCHIVE_DIR}/"
                archiveArtifacts artifacts: '*.tar.gz', onlyIfSuccessful: true
            }
        }
    }
}