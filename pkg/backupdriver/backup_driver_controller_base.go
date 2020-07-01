/*
Copyright 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backupdriver

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	backupdriverapi "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/apis/backupdriver/v1"
	backupdriverinformers_v1 "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/generated/informers/externalversions/backupdriver/v1"
	backupdriverlisters "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/generated/listers/backupdriver/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// BackupDriverController is the interface for backup driver controller
type BackupDriverController interface {
	// Run starts the controller.
	Run(ctx context.Context, workers int)
}

type backupDriverController struct {
	name   string
	logger logrus.FieldLogger
	// KubeClient of the current cluster
	kubeClient kubernetes.Interface

	rateLimiter workqueue.RateLimiter

	// PV Lister
	pvLister corelisters.PersistentVolumeLister
	// PV Synced
	pvSynced cache.InformerSynced

	// PVC Lister
	pvcLister corelisters.PersistentVolumeClaimLister
	// PVC Synced
	pvcSynced cache.InformerSynced
	// claim queue
	claimQueue workqueue.RateLimitingInterface

	// Supervisor Cluster PVC Lister
	svcPVCLister corelisters.PersistentVolumeClaimLister
	// Supervisor Cluster PVC Synced
	svcPVCSynced cache.InformerSynced

	// Snapshot queue
	snapshotQueue workqueue.RateLimitingInterface
	// Snapshot Lister
	snapshotLister backupdriverlisters.SnapshotLister
	// Snapshot Synced
	snapshotSynced cache.InformerSynced
}

// NewBackupDriverController returns a BackupDriverController.
func NewBackupDriverController(
	name string,
	logger logrus.FieldLogger,
	// Kubernetes Cluster KubeClient
	kubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
	informerFactory informers.SharedInformerFactory,
	rateLimiter workqueue.RateLimiter,
	snapshotInformer backupdriverinformers_v1.SnapshotInformer) BackupDriverController {
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	claimQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver")
	snapshotQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver")

	rc := &backupDriverController{
		name:       name,
		logger:     logger.WithField("controller", name),
		kubeClient: kubeClient,
		pvLister:   pvInformer.Lister(),
		pvSynced:   pvInformer.Informer().HasSynced,
		pvcLister:  pvcInformer.Lister(),
		pvcSynced:  pvcInformer.Informer().HasSynced,
		claimQueue: claimQueue,
		//snapshotLister: snapshotInformer.Lister(),
		//snapshotSynced: snapshotInformer.Informer().HasSynced,
		snapshotQueue: snapshotQueue,
	}

	pvcInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.addPVC,
		UpdateFunc: rc.updatePVC,
		//DeleteFunc: rc.deletePVC,
	}, resyncPeriod)

	pvInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.addPV,
		//UpdateFunc: rc.updatePV,
		//DeleteFunc: rc.deletePV,
	}, resyncPeriod)

	//rc.snapshotLister = snapshotInformer.Lister()
	//rc.snapshotSynced = snapshotInformer.Informer().HasSynced

	snapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { rc.addSnapshot(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { rc.updateSnapshot(oldObj, newObj) },
			DeleteFunc: func(obj interface{}) { rc.deleteSnapshot(obj) },
		},
		resyncPeriod,
	)

	/*snapshotInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.addSnapshot,
		UpdateFunc: rc.updateSnapshot,
		DeleteFunc: rc.deleteSnapshot,
	}, resyncPeriod)*/

	rc.snapshotLister = snapshotInformer.Lister()
	rc.snapshotSynced = snapshotInformer.Informer().HasSynced

	return rc
}

func (rc *backupDriverController) addPVC(obj interface{}) {
	rc.logger.Infof("addPVC: %+v", obj)
	objKey, err := rc.getKey(obj)
	if err != nil {
		return
	}
	rc.logger.Infof("addPVC: add %s to PVC queue", objKey)
	rc.claimQueue.Add(objKey)
}

func (rc *backupDriverController) addSnapshot(obj interface{}) {
	rc.logger.Infof("addSnapshot: %+v", obj)
	objKey, err := rc.getKey(obj)
	if err != nil {
		return
	}
	rc.logger.Infof("addSnapshot: add %s to Snapshot queue", objKey)
	rc.snapshotQueue.Add(objKey)
}

// getKey helps to get the resource name from resource object
func (rc *backupDriverController) getKey(obj interface{}) (string, error) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	objKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		rc.logger.Errorf("Failed to get key from object: %v", err)
		return "", err
	}
	rc.logger.Infof("getKey: key %s", objKey)
	return objKey, nil
}

func (rc *backupDriverController) updatePVC(oldObj, newObj interface{}) {
	rc.logger.Infof("updatePVC: old [%+v] new [%+v]", oldObj, newObj)
	oldPVC, ok := oldObj.(*v1.PersistentVolumeClaim)
	if !ok || oldPVC == nil {
		return
	}
	rc.logger.Infof("updatePVC: old PVC %+v", oldPVC)

	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok || newPVC == nil {
		return
	}
	rc.logger.Infof("updatePVC: new PVC %+v", newPVC)

	rc.addPVC(newObj)
}

func (rc *backupDriverController) updateSnapshot(oldObj, newObj interface{}) {
	rc.logger.Infof("updateSnapshot: old [%+v] new [%+v]", oldObj, newObj)
	oldSnapshot, ok := oldObj.(*backupdriverapi.Snapshot)
	if !ok || oldSnapshot == nil {
		return
	}
	rc.logger.Infof("updateSnapshot: old Snapshot %+v", oldSnapshot)

	newSnapshot, ok := newObj.(*backupdriverapi.Snapshot)
	if !ok || newSnapshot == nil {
		return
	}
	rc.logger.Infof("updateSnapshot: new Snapshot %+v", newSnapshot)

	rc.addSnapshot(newObj)
}

func (rc *backupDriverController) deleteSnapshot(obj interface{}) {
}

// Run starts the controller.
func (rc *backupDriverController) Run(
	ctx context.Context, workers int) {
	defer rc.claimQueue.ShutDown()
	defer rc.snapshotQueue.ShutDown()

	rc.logger.Infof("Starting backup driver controller")
	defer rc.logger.Infof("Shutting down backup driver controller")

	stopCh := ctx.Done()

	rc.logger.Infof("Before sync")

	if !cache.WaitForCacheSync(stopCh, rc.pvSynced, rc.pvcSynced, rc.snapshotSynced) {
		rc.logger.Errorf("Cannot sync caches")
		return
	}
	rc.logger.Infof("After sync. It works!!!")

	for i := 0; i < workers; i++ {
		rc.logger.Infof("Backup driver worker: %d", i+1)
		go wait.Until(rc.pvcWorker, 0, stopCh)
		go wait.Until(rc.snapshotWorker, 0, stopCh)
	}

	<-stopCh
}

// pvcWorker is the main worker for PVC request.
func (rc *backupDriverController) pvcWorker() {
	rc.logger.Infof("pvcWorker: Enter pvcWorker")

	key, quit := rc.claimQueue.Get()
	if quit {
		return
	}
	defer rc.claimQueue.Done(key)

	//if err := rc.syncSnapshot(key.(string)); err != nil {
	//        // Put snapshot back to the queue so that we can retry later.
	//        rc.snapshotQueue.AddRateLimited(key)
	//} else {
	//        rc.snapshotQueue.Forget(key)
	//}
	rc.logger.Infof("pvcWorker: Exit pvcWorker")
}

// snapshotWorker is the main worker for snapshot request.
func (rc *backupDriverController) snapshotWorker() {
	rc.logger.Infof("snapshotWorker: Enter snapshotWorker")

	key, quit := rc.snapshotQueue.Get()
	if quit {
		return
	}
	defer rc.snapshotQueue.Done(key)

	if err := rc.syncSnapshot(key.(string)); err != nil {
		// Put snapshot back to the queue so that we can retry later.
		rc.snapshotQueue.AddRateLimited(key)
	} else {
		rc.snapshotQueue.Forget(key)
	}
	rc.logger.Infof("snapshotWorker: Exit snapshotWorker")
}

// syncSnapshot processes one Snapshot CRD
func (rc *backupDriverController) syncSnapshot(key string) error {
	rc.logger.Infof("syncSnapshot: Started Snapshot processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		rc.logger.Errorf("Split meta namespace key of snapshot %s failed: %v", key, err)
		return err
	}

	// TODO: Replace _ with snapshot when we start to use it
	snapshot, err := rc.snapshotLister.Snapshots(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			rc.logger.Infof("Snapshot %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		rc.logger.Errorf("Get Snapshot %s/%s failed: %v", namespace, name, err)
		return err
	}

	// TODO: Call other function to process create snapshot request
	rc.logger.Infof("syncSnapshot: print out snapshot info %+v", snapshot)

	return nil
}
