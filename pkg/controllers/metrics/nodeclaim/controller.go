package nodeclaim

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

type Controller struct {
	kubeClient client.Client
}

func NewController(kubeClient client.Client) *Controller {
	return &Controller{
		kubeClient: kubeClient,
	}
}

func (c *Controller) Name() string {
	return "nodeclaim-metrics"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("metrics").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func (c *Controller) Reconcile(ctx context.Context) (reconcile.Result, error) {
	nodeClaimList := &v1.NodeClaimList{}
	if err := c.kubeClient.List(ctx, nodeClaimList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list node claims")
		return reconcile.Result{}, err
	}

	nodePoolMap := make(map[string]map[string]int)
	for _, nodeClaim := range nodeClaimList.Items {
		nodePoolName := nodeClaim.Labels[v1.NodePoolLabelKey]
		if _, ok := nodePoolMap[nodePoolName]; !ok {
			nodePoolMap[nodePoolName] = map[string]int{
				"created":     0,
				"launched":    0,
				"registered":  0,
				"initialized": 0,
				"disrupted":   0,
				"terminated":  0,
			}
		}

		if nodeClaim.DeletionTimestamp != nil && !nodeClaim.DeletionTimestamp.IsZero() ||
			nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			nodePoolMap[nodePoolName]["terminated"] += 1
			continue
		}
		if nodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue() ||
			nodeClaim.StatusConditions().Get(v1.ConditionTypeDisruptionReason).IsTrue() {
			nodePoolMap[nodePoolName]["disrupted"] += 1
			continue
		}
		if nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized).IsTrue() {
			nodePoolMap[nodePoolName]["initialized"] += 1
			continue
		}
		if nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() {
			nodePoolMap[nodePoolName]["registered"] += 1
			continue
		}
		if nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched).IsTrue() {
			nodePoolMap[nodePoolName]["launched"] += 1
			continue
		}
		nodePoolMap[nodePoolName]["created"] += 1
	}

	for nodePoolName, conditionMap := range nodePoolMap {
		for conditionType, count := range conditionMap {
			metrics.NodeClaimsConditionGauge.Set(float64(count), map[string]string{metrics.ConditionLabel: conditionType, metrics.NodePoolLabel: nodePoolName})
		}
	}
	return reconcile.Result{RequeueAfter: 1 * time.Minute}, nil
}
