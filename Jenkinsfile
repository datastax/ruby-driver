def initializeEnvironment() {

    env.DRIVER_DISPLAY_NAME = 'Cassandra Ruby Driver'
    env.DRIVER_METRIC_TYPE = 'oss'

    env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
    env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
    env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
    env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"

    sh label: 'Download Apache CassandraⓇ or DataStax Enterprise', script: '''#!/bin/bash -lex
        . ${CCM_ENVIRONMENT_SHELL} ${CASSANDRA_VERSION}
    '''
}

def describeBuild() {}

def installDependencies() {
    sh label: 'Update bundler', script: '''#!/bin/bash -le
        bundle update --bundler
    '''
    sh label: 'Update dependent gems', script: '''#!/bin/bash -le
        bundle --version
        bundle install --without development docs
    '''
}

def buildDriver() {}

def executeTests() {
    sh label: 'Execute all tests', script: '''#!/bin/bash -le
        bundle exec rake test
    '''
}

pipeline {
    agent none

    // Global pipeline timeout
    options {
	timeout(time: 10, unit: 'HOURS')
	buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
				  numToKeepStr: '50'))        // Keep only the last 50 build records
    }

    environment {
	CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    }

    stages {
	stage('Per-Commit') {
	    environment {
		OS_VERSION = 'ubuntu/bionic64/ruby-driver'
	    }

	    matrix {
		axes {
		    axis {
			name 'CASSANDRA_VERSION'
			values '2.1', '3.11', '4.0'
		    }
		    axis {
			name 'RUBY_VERSION'
			values '2.3.6', '2.4.3', '2.7.0', 'jruby-9.1.15.0'
		    }
		}
		agent {
		    label "${OS_VERSION}"
		}

		stages {
		    stage('Initialize-Environment') {
			steps {
			    initializeEnvironment()
			}
		    }
		    stage('Describe-Build') {
			steps {
			    describeBuild()
			}
		    }
		    stage('Install-Dependencies') {
			steps {
			    installDependencies()
			}
		    }
		    stage('Build-Driver') {
			steps {
			    buildDriver()
			}
		    }
		    stage('Execute-Tests') {
			steps {
			    executeTests()
			}
		    }		    
		}
	    }
	}
    }
}
