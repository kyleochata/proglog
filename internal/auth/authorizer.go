package auth

import (
	"fmt"
	"log"

	casbin "github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *Authorizer {
	// log.Printf("creating enforcer from casbin:")
	// log.Printf("using model file: %s", model)
	// log.Printf("using policy file: %s", policy)

	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		log.Fatalf("failed to create casbin enforcer: %v", err)
	}

	if enforcer == nil {
		log.Fatalf("casbin enforcer is nil after creation")
	}

	log.Printf("enforcer successfully created: %v", enforcer)
	return &Authorizer{
		enforcer: enforcer,
	}
}

// Authorize defers to Casbin's Enforce func. Enforce returns whether the given subject is permitted to run the given action on the given object based on the model and policy Casbin auth mechanism and policy is configured with. (ACL for both)
func (a *Authorizer) Authorize(subject, object, action string) error {
	// log.Printf("Checking permission for subject: %s, object: %s, action: %s", subject, object, action)
	// if a.enforcer == nil {
	// 	log.Printf("enforcer in Authorizer is nil: %v", a)
	// }

	result, _ := a.enforcer.Enforce(subject, object, action)

	if !result {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	// log.Printf("finished checking perm for subject: %s, object: %s, action: %s", subject, object, action)
	return nil
}
