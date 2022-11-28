package io.trino.tests.product.launcher.env.environment;

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

@TestsEnvironment
public class EnvMultinodeBigQuery
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeBigQuery(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-bigquery/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

}
