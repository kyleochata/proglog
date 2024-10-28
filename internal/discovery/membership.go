package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership type wraps Serf to provide discovery and cluster membership to our service.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	serfMembership := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := serfMembership.setupSerf(); err != nil {
		return nil, err
	}
	return serfMembership, nil
}

type Config struct {
	//unique id across Serf cluster. Serf uses hostname if not set
	NodeName string
	//Serf listens at this address and port for gossiping
	BindAddr string
	//Serf shares tags to other nodes in cluster and should use these tags for simple data that informs the cluster how to handle this node.
	Tags map[string]string
	//Configure new nodes to join an existing cluster. Set the field to the addresses of nodes in the cluster, and Serf's gossip protocol takes care of the rest to join your node to the cluster. (in prod: specify at least 3 addresses to make cluster resilient to one or two node failures or a disrupted network)
	StartJoinAddrs []string
}

// setupSerf creates and configures a Serf instance and starts the eventsHandler go routine to handle Serf's events
func (m *Membership) setupSerf() (err error) {
	//
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)

	//Receive Serf events when a node joins or leaves the cluster.
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handler represents some component in the service that needs to know when a server joins or leaves the cluster.
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// eventHandler runs a in a loop reading events sent by Serf into the event chan, handling each incoming event according to the event's type.
func (m *Membership) eventHandler() {
	//serf may combine multiple members updates into one event. Must iterate
	for e := range m.events {
		//When a node joins or leaves the cluster, Serf sends an event to all nodes, including the node that joins or leaves.
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				//check if event for the node is the current server
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal returns whether the given Serf member is the local member by checking the members' names
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members return a point-in-time snapshot of thecluster's Serf members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells the Serf member to leave the Serf cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs the given error and message. Raft will error and return ErrNotLeader when you try to change the cluster on non-leader nodes. In discovery service log all handler errors as critical, but if the node is a non-leader, then we should expect these errors and not log them. Non-leader errors will be logged at the debug level.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
