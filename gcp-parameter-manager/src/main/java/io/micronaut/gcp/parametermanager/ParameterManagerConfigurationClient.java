/*
 * Copyright 2017-2025 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.gcp.parametermanager;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import io.micronaut.context.annotation.BootstrapContextCompatible;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.context.env.EnvironmentPropertySource;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.PropertySourceLoader;
import io.micronaut.context.env.PropertySourceReader;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.discovery.config.ConfigurationClient;
import io.micronaut.gcp.parametermanager.client.ParameterManagerAccessClient;
import io.micronaut.gcp.parametermanager.client.VersionedParameter;
import io.micronaut.gcp.parametermanager.configuration.ParameterManagerConfigurationProperties;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Distributed configuration client implementation that fetches application configuration files from Google Cloud Parameter Manager.
 *
 * @author Alfatah Bheda
 * @since 6.0.0
 */
@Singleton
@BootstrapContextCompatible
@Requires(property = ConfigurationClient.ENABLED, value = StringUtils.TRUE, defaultValue = StringUtils.FALSE)
public class ParameterManagerConfigurationClient implements ConfigurationClient {

    private static final String CAMEL_CASE_REGEX = "([a-z])([A-Z]+)";
    private static final String CAMEL_CASE_REPLACE = "$1_$2";
    private static final String DESCRIPTION = "GCP Parameter Manager Config Client";
    private static final String PROPERTY_SOURCE_SUFFIX = " (GCP ParameterManager)";
    private static final List<PropertySourceLoader> READERS = ServiceLoader.load(PropertySourceLoader.class)
            .stream().map(ServiceLoader.Provider::get).toList();
    private final ParameterManagerAccessClient parameterManagerAccessClient;
    private final ParameterManagerConfigurationProperties parameterManagerConfigurationProperties;

    public ParameterManagerConfigurationClient(ParameterManagerAccessClient parameterManagerAccessClient, ParameterManagerConfigurationProperties parameterManagerConfigurationProperties) {
        this.parameterManagerAccessClient = parameterManagerAccessClient;
        this.parameterManagerConfigurationProperties = parameterManagerConfigurationProperties;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public Publisher<PropertySource> getPropertySources(Environment environment) {
        return Flux.concat(resolveParameterConfigs(), resolveParameterKeys());
    }

    private Publisher<PropertySource> resolveParameterConfigs() {
        return Flux.fromIterable(configCandidates().entrySet())
            .flatMap(env -> {
                ParsedParameter parsedParameter = parseNameAndVersion(env.getValue());
                return Mono.from(parameterManagerAccessClient.getRenderedParameter(parsedParameter.name, parsedParameter.version))
                    .mapNotNull(parameter -> fromParameter(parameter, env.getKey()));
            }
        );
    }

    private Publisher<PropertySource> resolveParameterKeys() {
        return Flux.fromIterable(parameterManagerConfigurationProperties.getKeys())
                .flatMap(parameter -> {
                ParsedParameter parsedParameter = parseNameAndVersion(parameter);
                return parameterManagerAccessClient.getRenderedParameter(parsedParameter.name, parsedParameter.version);
            })
                .filter(Objects::nonNull)
                .collectMap(versionedParameter -> "pm." + versionedParameter.getName().replaceAll(CAMEL_CASE_REGEX, CAMEL_CASE_REPLACE).toUpperCase(),
                    versionedParameter -> (Object) new String(versionedParameter.getContents(), StandardCharsets.UTF_8).replaceAll("\\n", "").trim())
                .map(m -> PropertySource.of("parameter-manager-keys", m, PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE));
    }

    private Map<Integer, String> configCandidates() {
        Map<Integer, String> candidates = new HashMap<>();
        int priority = EnvironmentPropertySource.POSITION + 150;

        for (String name: parameterManagerConfigurationProperties.getCustomConfigs()) {
            candidates.put(++priority, name);
        }
        return candidates;
    }

    private PropertySource fromParameter(VersionedParameter parameter, int priority) {
        Map<String, Object> data = new HashMap<>();

        for (PropertySourceReader reader : READERS) {
            try {
                data.putAll(reader.read(parameter.getName(), parameter.getContents()));
                if (!data.isEmpty()) {
                    break;
                }
            } catch (Exception e) {
            }
        }
        return PropertySource.of(parameter.getName() + PROPERTY_SOURCE_SUFFIX, data, priority);
    }

    /**
     * Accepts input strictly in the format.
     *   parameter_name/parameter_version
     * Examples:
     *   "my-param/1"
     *   "my-param/latest"
     */
    private ParsedParameter parseNameAndVersion(String raw) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new ConfigurationException("Parameter reference must not be empty");
        }

        int idx = trimmed.lastIndexOf('/');

        if (idx < 0) {
            throw new ConfigurationException(
                "Invalid parameter format. Expected 'parameter_name/parameter_version' but got: " + raw
            );
        }

        String name = trimmed.substring(0, idx);
        String version = trimmed.substring(idx + 1);

        if (name.isBlank()) {
            throw new ConfigurationException("Parameter name must not be empty: " + raw);
        }
        if (name.contains("/")) {
            throw new ConfigurationException("Parameter name must not contain '/': " + raw);
        }

        if (version.isBlank()) {
            throw new ConfigurationException("Parameter version must not be empty: " + raw);
        }

        return new ParsedParameter(name, version);
    }

    private record ParsedParameter(String name, String version) {
    }
}
