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
        string(name: 'PROJECT_NAME',
            defaultValue: 'Pirates/Server/pirates-server/master',
            description: 'Full name of the build project. Example: Pirates/Server/pirates-server/master')
        string(name: 'PROJECT_BUILD_NUMBER',
            defaultValue: '',
            description: 'Which build number to copy')
    }

    environment {
        RETENTION_BUILD_SCRIPT_DIR = "${env.WORKSPACE}/retention"
        RETENTION_LIB_DIR = "${env.WORKSPACE}/libs"
        RETENTION_ARCHIVE_DIR = "${env.WORKSPACE}/retention-archive"
        RETENTION_ZIP_NAME = "rentention.zip"
        RETENTION_ZIP = "${env.RETENTION_ARCHIVE_DIR}/${env.RETENTION_ZIP_NAME}"
    }

    stages {
        stage('Clean Libs') {
            steps {
                 sh """
                    rm -rf ${env.RETENTION_LIB_DIR}
                    mdir -p  ${env.RETENTION_LIB_DIR}
                    mdir -p  ${env.RETENTION_ARCHIVE_DIR}
                    rm  ${env.RETENTION_ZIP}
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
                        pip3 install -r requirements.txt -t ${env.RETENTION_LIB_DIR}
                        zip -r9 ${env.RETENTION_ZIP} ${env.RETENTION_LIB_DIR}/*
                        zip -g ${env.RETENTION_ZIP} * -x requirements.txt setup.cfg README.md
                    """
                }
            }
        }
        stage('Archive') {
            steps {
                zip archive: true, dir: env.RETENTION_ARCHIVE_DIR, zipFile: env.RETENTION_ZIP_NAME
            }
        }
    }
}