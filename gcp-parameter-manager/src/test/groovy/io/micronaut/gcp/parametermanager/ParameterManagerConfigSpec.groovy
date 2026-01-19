package io.micronaut.gcp.parametermanager

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import spock.lang.Specification
import spock.util.environment.RestoreSystemProperties

@RestoreSystemProperties
class ParameterManagerConfigSpec extends Specification {

    void "load first project"() {
        given:
            System.setProperty(Environment.BOOTSTRAP_CONTEXT_PROPERTY, "true")
            ApplicationContext context = ApplicationContext.run(["spec.name"                              : "ParameterManagerConfigSpec",
                                                                 "micronaut.application.name"             : "parameter-manager-test",
                                                                 "micronaut.config-client.enabled"        : true,
                                                                 "gcp.projectId"                          : "first-gcp-project",
                                                                 "gcp.parameter-manager.custom-configs[0]": "microParam/v1"])
        expect:
            context.containsProperties("custom.value")
        cleanup:
            context.stop()
    }
}
