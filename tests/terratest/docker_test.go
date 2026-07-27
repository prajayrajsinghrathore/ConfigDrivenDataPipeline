package test

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/docker"
	"github.com/stretchr/testify/require"
)

func TestDockerBuild(t *testing.T) {
	// Setup the Docker build options
	// We run from the tests/terratest directory, so the Docker context is ../../
	buildOptions := &docker.BuildOptions{
		Tags: []string{"config-driven-data-pipeline:test"},
		BuildArgs: []string{
			"PYTHON_VERSION=3.13",
			// Using false so tests don't require internal certs to pass
			"USE_ZSCALER_CERT=false",
		},
	}

	// Build the Docker image, and assert there are no errors
	err := docker.BuildE(t, "../../", buildOptions)
	require.NoError(t, err, "Failed to build Docker image")
}
