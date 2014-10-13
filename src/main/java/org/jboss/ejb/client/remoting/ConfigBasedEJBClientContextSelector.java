/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ejb.client.remoting;

import org.jboss.ejb.client.ClusterContext;
import org.jboss.ejb.client.ClusterNodeManager;
import org.jboss.ejb.client.EJBClientConfiguration;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.ejb.client.EJBClientContextIdentifier;
import org.jboss.ejb.client.EJBReceiver;
import org.jboss.ejb.client.IdentityEJBClientContextSelector;
import org.jboss.ejb.client.Logs;
import org.jboss.logging.Logger;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.Remoting;
import org.jboss.remoting3.remote.RemoteConnectionProviderFactory;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

import javax.security.auth.callback.CallbackHandler;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * An EJB client context selector which uses {@link EJBClientConfiguration} to create {@link org.jboss.ejb.client.remoting.RemotingConnectionEJBReceiver}s.
 *
 * @author Jaikiran Pai
 */
public class ConfigBasedEJBClientContextSelector implements IdentityEJBClientContextSelector {

    private static final Logger logger = Logger.getLogger(ConfigBasedEJBClientContextSelector.class);
    private static final InetAddress localHost;

    static {
      try {
        localHost = InetAddress.getLocalHost();
      } catch(IOException ioe) {
        throw new IllegalStateException("Unable to fetch localhost", ioe);
      }
    }

    private final EJBClientConfiguration ejbClientConfiguration;
    private final EJBClientContext ejbClientContext;
    private final RemotingCleanupHandler remotingCleanupHandler = new RemotingCleanupHandler();

    private final ConcurrentMap<EJBClientContextIdentifier, EJBClientContext> identifiableContexts = new ConcurrentHashMap<EJBClientContextIdentifier, EJBClientContext>();

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration) {
        this(ejbClientConfiguration, null);
    }

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     * @param classLoader The classloader that will be used to {@link EJBClientContext#create(org.jboss.ejb.client.EJBClientConfiguration, ClassLoader) create the EJBClientContext}
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration, final ClassLoader classLoader) {
      this(ejbClientConfiguration, classLoader, null);
    }

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     * @param classLoader The classloader that will be used to {@link EJBClientContext#create(org.jboss.ejb.client.EJBClientConfiguration, ClassLoader) create the EJBClientContext}
     * @param connectionCreationStrategy The connection creation strategy to use for the cluster. Null allowed.
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration, final ClassLoader classLoader, final ClusterContext.ConnectionCreationStrategy connectionCreationStrategy) {
      this(ejbClientConfiguration, classLoader, connectionCreationStrategy, null);
    }

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     * @param classLoader The classloader that will be used to {@link EJBClientContext#create(org.jboss.ejb.client.EJBClientConfiguration, ClassLoader) create the EJBClientContext}
     * @param connectionCreationStrategy The connection creation strategy to use for the cluster. Null allowed.
     * @param intializer The Initializer to use or null for default. Null allowed.
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration, final ClassLoader classLoader, final ClusterContext.ConnectionCreationStrategy connectionCreationStrategy, final ConfigBasedEJBClientContextSelector.Initializer initializer) {
        this.ejbClientConfiguration = ejbClientConfiguration;
        // create a empty context
        if (classLoader == null) {
            this.ejbClientContext = EJBClientContext.create(this.ejbClientConfiguration, connectionCreationStrategy);
        } else {
            this.ejbClientContext = EJBClientContext.create(this.ejbClientConfiguration, classLoader, connectionCreationStrategy);
        }
        // register a EJB client context listener which we will use to close endpoints/connections that we created,
        // when the EJB client context closes
        this.ejbClientContext.registerEJBClientContextListener(this.remotingCleanupHandler);

        // now setup the receivers (if any) for the context
        if (this.ejbClientConfiguration == null) {
            logger.debug("EJB client context " + this.ejbClientContext + " will have no EJB receivers associated with it since there was no " +
                    "EJB client configuration available to create the receivers");
            return;
        }
        try {
            (initializer != null ? initializer : new DefaultInitializer()).initialize(this, this.ejbClientContext);
        } catch (IOException ioe) {
            logger.warn("EJB client context " + this.ejbClientContext + " will have no EJB receivers due to an error setting up EJB receivers", ioe);
        }
    }

    @Override
    public EJBClientContext getCurrent() {
        return this.ejbClientContext;
    }

    private void trackEndpoint(final Endpoint endpoint) {
        if (endpoint == null) {
            return;
        }
        // Let the RemotingCleanupHandler which is a EJB client context listener be made
        // aware of the endpoint, so that it can be closed when the EJB client context is closed
        this.remotingCleanupHandler.addEndpoint(endpoint);
        // also let the AutoConnectionCloser to track this endpoint, in case no one calls EJBClientContext.close()
        // so that AutoConnectionCloser can then close the endpoint on JVM runtime shutdown
        AutoConnectionCloser.INSTANCE.addEndpoint(endpoint);
    }

    private void trackConnection(final Connection connection) {
        if (connection == null) {
            return;
        }
        // Let the RemotingCleanupHandler which is a EJB client context listener be made
        // aware of the connection, so that it can be closed when the EJB client context is closed
        this.remotingCleanupHandler.addConnection(connection);
        // also let the AutoConnectionCloser to track this connection, in case no one calls EJBClientContext.close()
        // so that AutoConnectionCloser can then close the connection on JVM runtime shutdown
        AutoConnectionCloser.INSTANCE.addConnection(connection);
    }

    @Override
    public void registerContext(final EJBClientContextIdentifier identifier, final EJBClientContext context) {
        final EJBClientContext previousRegisteredContext = this.identifiableContexts.putIfAbsent(identifier, context);
        if (previousRegisteredContext != null) {
            throw Logs.MAIN.ejbClientContextAlreadyRegisteredForIdentifier(identifier);
        }
    }

    @Override
    public EJBClientContext unRegisterContext(final EJBClientContextIdentifier identifier) {
        return this.identifiableContexts.remove(identifier);
    }

    @Override
    public EJBClientContext getContext(final EJBClientContextIdentifier identifier) {
        return this.identifiableContexts.get(identifier);
    }

    /**
     * Initializes the {@link ConfigBasedEJBClientContextSelector}, e.g. setting up {@link EJBReceiver}s.
     *
     * @author kristoffer@cambio.se
     */
    public abstract static class Initializer {
      /**
       * Creates an {@link Endpoint} based on the supplied paramaters, and tracks the created instance.
       */
      protected Endpoint createEndpoint(final ConfigBasedEJBClientContextSelector selector, final String endpointName, final OptionMap endpointCreationOptions, final OptionMap remoteConnectionProviderOptions) throws IOException {
        // create the endpoint
        final Endpoint endpoint = Remoting.createEndpoint(endpointName, endpointCreationOptions);
        // Keep track of this endpoint for closing on shutdown/client context close
        selector.trackEndpoint(endpoint);
        // register the remote connection provider
        endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), remoteConnectionProviderOptions);
        return endpoint;
      }

      /**
       * Creates a {@link Connection} using the supplied parameters and tracks the {@link Connection}.
       */
      protected Connection createConnection(final ConfigBasedEJBClientContextSelector selector, final Endpoint endpoint, final String host, final int port, final OptionMap createOptions, final long connectionTimeout, final CallbackHandler callbackHandler) throws IOException {
        final IoFuture<Connection> futureConnection = NetworkUtil.connect(endpoint, host, port, null, createOptions, callbackHandler, null);
        // wait for the connection to be established
        final Connection connection = IoFutureHelper.get(futureConnection, connectionTimeout, TimeUnit.MILLISECONDS);
        // keep track of the created connection for auto-close on shutdown/client context close
        selector.trackConnection(connection);
        return connection;
      }

      /**
       * Creates a {@link ClusterNodeManager} using the supplied parameters, with a {@link ClusterNode} with a single {@link ClientMapping} using localhost as sourceNetworkAddress and zero as sourceNetworkMaskBits.
       */
      protected ClusterNodeManager createClusterNodeManager(final ConfigBasedEJBClientContextSelector selector, final ClusterContext clusterContext, final String clusterName, final String nodeName, final String destinationAddress, final int destinationPort, final Endpoint endpoint, final EJBClientConfiguration ejbClientConfiguration) {
        return createClusterNodeManager(selector, clusterContext, clusterName, nodeName, new ClientMapping[] { new ClientMapping(localHost, 0, destinationAddress, destinationPort) }, endpoint, ejbClientConfiguration);
      }

      /**
       * Creates a {@link ClusterNodeManager} using the supplied parameters.
       */
      protected ClusterNodeManager createClusterNodeManager(final ConfigBasedEJBClientContextSelector selector, final ClusterContext clusterContext, final String clusterName, final String nodeName, final ClientMapping[] clientMappings, final Endpoint endpoint, final EJBClientConfiguration ejbClientConfiguration) {
        return new RemotingConnectionClusterNodeManager(clusterContext, new ClusterNode(clusterName, nodeName, clientMappings), endpoint, ejbClientConfiguration);
      }

      /**
       * Initializes the {@link ConfigBasedEJBClientContextSelector}, e.g. creating {@link EJBReceiver}s.
       */
      public abstract void initialize(final ConfigBasedEJBClientContextSelector selector, final EJBClientContext ejbClientContext) throws IOException;
    }

    /**
     * The default {@link Initializer}, creating initial connections to connect to the cluster, before receiving a cluster view.
     *
     * @author kristoffer@cambio.se
     */
    protected static class DefaultInitializer extends Initializer {
      public DefaultInitializer() {
      }

      @Override
      public void initialize(final ConfigBasedEJBClientContextSelector selector, final EJBClientContext ejbClientContext) throws IOException {
          final EJBClientConfiguration ejbClientConfiguration = ejbClientContext.getEJBClientConfiguration();

          if (!ejbClientConfiguration.getConnectionConfigurations().hasNext()) {
            // no connections configured so no EJB receivers to create
            return;
          }

          final Endpoint endpoint = this.createEndpoint(selector, ejbClientConfiguration.getEndpointName(), ejbClientConfiguration.getEndpointCreationOptions(), ejbClientConfiguration.getRemoteConnectionProviderCreationOptions());
          final Iterator<EJBClientConfiguration.RemotingConnectionConfiguration> connectionConfigurations = ejbClientConfiguration.getConnectionConfigurations();
          int successfulEJBReceiverRegistrations = 0;
          while (connectionConfigurations.hasNext()) {
              ReconnectHandler reconnectHandler = null;
              final EJBClientConfiguration.RemotingConnectionConfiguration connectionConfiguration = connectionConfigurations.next();
              final String host = connectionConfiguration.getHost();
              final int port = connectionConfiguration.getPort();
              try {
                  final OptionMap connectionCreationOptions = connectionConfiguration.getConnectionCreationOptions();
                  final CallbackHandler callbackHandler = connectionConfiguration.getCallbackHandler();
                  // create a re-connect handler (which will be used on connection breaking down)
                  final int MAX_RECONNECT_ATTEMPTS = 65535; // TODO: Let's keep this high for now and later allow configuration and a smaller default value
                  reconnectHandler = new EJBClientContextConnectionReconnectHandler(ejbClientContext, endpoint, host, port, connectionCreationOptions, callbackHandler, connectionConfiguration.getChannelCreationOptions(), MAX_RECONNECT_ATTEMPTS,
                          connectionConfiguration.getConnectionTimeout(), TimeUnit.MILLISECONDS);

                  final Connection connection = this.createConnection(selector, endpoint, host, port, connectionCreationOptions, connectionConfiguration.getConnectionTimeout(), callbackHandler);
                  // create a remoting EJB receiver for this connection
                  final EJBReceiver remotingEJBReceiver = new RemotingConnectionEJBReceiver(connection, reconnectHandler, connectionConfiguration.getChannelCreationOptions());
                  // associate it with the client context
                  ejbClientContext.registerEJBReceiver(remotingEJBReceiver);
                  // keep track of successful registrations for logging purposes
                  successfulEJBReceiverRegistrations++;
              } catch (Exception e) {
                  // just log the warn but don't throw an exception. Move onto the next connection configuration (if any)
                  logger.warn("Could not register a EJB receiver for connection to " + host + ":" + port, e);
                  // add a reconnect handler for this connection
                  if (reconnectHandler != null) {
                      ejbClientContext.registerReconnectHandler(reconnectHandler);
                      logger.debug("Registered a reconnect handler in EJB client context " + ejbClientContext + " for remote://" + host + ":" + port);
                  }
              }
          }
          logger.debug("Registered " + successfulEJBReceiverRegistrations + " remoting EJB receivers for EJB client context " + ejbClientContext);
        }
    }
}
