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
	//"fmt"
	"time"

	"k8s.io/client-go/rest"

	"github.com/sirupsen/logrus"
	backupdriverapi "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/apis/backupdriver/v1"
	backupdriverclientset "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/generated/clientset/versioned/typed/backupdriver/v1"
	backupdriverinformers "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/generated/informers/externalversions"
	backupdriverlisters "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/generated/listers/backupdriver/v1"
	"github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/snapshotmgr"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Supervisor Cluster KubeClient from Guest Cluster
	svcKubeClient kubernetes.Interface

	// Supervisor Cluster KubeClient for Guest Cluster
	svcKubeConfig *rest.Config

	backupdriverClient *backupdriverclientset.BackupdriverV1Client

	// Supervisor Cluster namespace
	supervisorNamespace string

	rateLimiter workqueue.RateLimiter

	// PV Lister
	pvLister corelisters.PersistentVolumeLister
	// PV Synced
	pvSynced cache.InformerSynced

	// PVC Lister
	pvcLister corelisters.PersistentVolumeClaimLister
	// PVC Synced
	pvcSynced cache.InformerSynced
	// PVC queue
	pvcQueue workqueue.RateLimitingInterface

	// Supervisor Cluster PVC Lister
	svcPVCLister corelisters.PersistentVolumeClaimLister
	// Supervisor Cluster PVC Synced
	svcPVCSynced cache.InformerSynced

	// BackupRepositoryClaim Lister
	backupRepositoryClaimLister backupdriverlisters.BackupRepositoryClaimLister
	// BackupRepositoryClaim Synced
	backupRepositoryClaimSynced cache.InformerSynced
	// backupRepositoryClaim queue
	backupRepositoryClaimQueue workqueue.RateLimitingInterface

	// Supervisor Cluster BackupRepositoryClaim Lister
	svcBackupRepositoryClaimLister backupdriverlisters.BackupRepositoryClaimLister
	// Supervisor Cluster BackupRepositoryClaim Synced
	svcBackupRepositoryClaimSynced cache.InformerSynced

	// BackupRepository Lister
	backupRepositoryLister backupdriverlisters.BackupRepositoryLister
	// BackupRepository Synced
	backupRepositorySynced cache.InformerSynced

	// Snapshot queue
	snapshotQueue workqueue.RateLimitingInterface
	// Snapshot Lister
	snapshotLister backupdriverlisters.SnapshotLister
	// Snapshot Synced
	snapshotSynced cache.InformerSynced

	// Supervisor Cluster Snapshot Lister
	svcSnapshotLister backupdriverlisters.SnapshotLister
	// Supervisor Cluster Snapshot Synced
	svcSnapshotSynced cache.InformerSynced

	// CloneFromSnapshot queue
	cloneFromSnapshotQueue workqueue.RateLimitingInterface
	// CloneFromSnapshot Lister
	cloneFromSnapshotLister backupdriverlisters.CloneFromSnapshotLister
	// CloneFromSnapshot Synced
	cloneFromSnapshotSynced cache.InformerSynced

	// Supervisor Cluster CloneFromSnapshot Lister
	svcCloneFromSnapshotLister backupdriverlisters.CloneFromSnapshotLister
	// Supervisor Cluster CloneFromSnapshot Synced
	svcCloneFromSnapshotSynced cache.InformerSynced

	// Snapshot manager
	snapManager *snapshotmgr.SnapshotManager
}

// NewBackupDriverController returns a BackupDriverController.
func NewBackupDriverController(
	name string,
	logger logrus.FieldLogger,
	// Kubernetes Cluster KubeClient
	kubeClient kubernetes.Interface,
	backupdriverClient *backupdriverclientset.BackupdriverV1Client,
	svcKubeConfig *rest.Config,
	supervisorNamespace string,
	resyncPeriod time.Duration,
	informerFactory informers.SharedInformerFactory,
	backupdriverInformerFactory backupdriverinformers.SharedInformerFactory,
	snapManager *snapshotmgr.SnapshotManager,
	rateLimiter workqueue.RateLimiter) BackupDriverController {
	// TODO: Fix svcPVCInformer to use svcInformerFactory
	svcPVCInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	//svcPVCInformer := svcInformerFactory.Core().V1().PersistentVolumeClaims()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()

	backupRepositoryInformer := backupdriverInformerFactory.Backupdriver().V1().BackupRepositories()

	backupRepositoryClaimInformer := backupdriverInformerFactory.Backupdriver().V1().BackupRepositoryClaims()
	// TODO: Use svcBackupdriverInformerFactory for svcBackupRepositoryClaimInformer
	svcBackupRepositoryClaimInformer := backupdriverInformerFactory.Backupdriver().V1().BackupRepositoryClaims()

	snapshotInformer := backupdriverInformerFactory.Backupdriver().V1().Snapshots()
	// TODO: Use svcBackupdriverInformerFactory for svcSnapshotInformer
	svcSnapshotInformer := backupdriverInformerFactory.Backupdriver().V1().Snapshots()

	cloneFromSnapshotInformer := backupdriverInformerFactory.Backupdriver().V1().CloneFromSnapshots()
	// TODO: Use svcBackupdriverInformerFactor for svcCloneFromSnapshotInformer
	svcCloneFromSnapshotInformer := backupdriverInformerFactory.Backupdriver().V1().CloneFromSnapshots()

	pvcQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver-pvc-queue")
	snapshotQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver-snapshot-queue")
	cloneFromSnapshotQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver-clone-queue")
	backupRepositoryClaimQueue := workqueue.NewNamedRateLimitingQueue(
		rateLimiter, "backup-driver-brc-queue")

	ctrl := &backupDriverController{
		name:                           name,
		logger:                         logger.WithField("controller", name),
		svcKubeConfig:                  svcKubeConfig,
		kubeClient:                     kubeClient,
		backupdriverClient:             backupdriverClient,
		snapManager:                    snapManager,
		pvLister:                       pvInformer.Lister(),
		pvSynced:                       pvInformer.Informer().HasSynced,
		pvcLister:                      pvcInformer.Lister(),
		pvcSynced:                      pvcInformer.Informer().HasSynced,
		pvcQueue:                     pvcQueue,
		svcPVCLister:                   svcPVCInformer.Lister(),
		svcPVCSynced:                   svcPVCInformer.Informer().HasSynced,
		supervisorNamespace:            supervisorNamespace,
		snapshotLister:                 snapshotInformer.Lister(),
		snapshotSynced:                 snapshotInformer.Informer().HasSynced,
		snapshotQueue:                  snapshotQueue,
		svcSnapshotLister:              svcSnapshotInformer.Lister(),
		svcSnapshotSynced:              svcSnapshotInformer.Informer().HasSynced,
		cloneFromSnapshotLister:        cloneFromSnapshotInformer.Lister(),
		cloneFromSnapshotSynced:        cloneFromSnapshotInformer.Informer().HasSynced,
		cloneFromSnapshotQueue:         cloneFromSnapshotQueue,
		svcCloneFromSnapshotLister:     svcCloneFromSnapshotInformer.Lister(),
		svcCloneFromSnapshotSynced:     svcCloneFromSnapshotInformer.Informer().HasSynced,
		backupRepositoryLister:         backupRepositoryInformer.Lister(),
		backupRepositorySynced:         backupRepositoryInformer.Informer().HasSynced,
		backupRepositoryClaimLister:    backupRepositoryClaimInformer.Lister(),
		backupRepositoryClaimSynced:    backupRepositoryClaimInformer.Informer().HasSynced,
		backupRepositoryClaimQueue:     backupRepositoryClaimQueue,
		svcBackupRepositoryClaimLister: svcBackupRepositoryClaimInformer.Lister(),
		svcBackupRepositoryClaimSynced: svcBackupRepositoryClaimInformer.Informer().HasSynced,
	}

	pvcInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueuePVC(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueuePVC(newObj) },
			//DeleteFunc: func(obj interface{}) { ctrl.delPVC(obj) },
		},
		resyncPeriod,
	)

	svcPVCInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.svcAddPVC,
		//UpdateFunc: rc.svcUpdatePVC,
		//DeleteFunc: rc.svcDeletePVC,
	}, resyncPeriod)

	pvInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.addPV,
		//UpdateFunc: rc.updatePV,
		//DeleteFunc: rc.deletePV,
	}, resyncPeriod)

	snapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) { ctrl.enqueueSnapshot(obj) },
			//UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueSnapshot(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.delSnapshot(obj) },
		},
		resyncPeriod,
	)

	svcSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.svcAddSnapshot,
		//UpdateFunc: rc.svcUpdateSnapshot,
		//DeleteFunc: rc.svcDeleteSnapshot,
	}, resyncPeriod)

	cloneFromSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) { ctrl.enqueueCloneFromSnapshot(obj) },
			//UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueCloneFromSnapshot(newObj) },
			//DeleteFunc: func(obj interface{}) { ctrl.delCloneFromSnapshot(obj) },
		},
		resyncPeriod,
	)

	svcCloneFromSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.svcAddCloneFromSnapshot,
		//UpdateFunc: rc.svcUpdateCloneFromSnapshot,
		//DeleteFunc: rc.svcDeleteCloneFromSnapshot,
	}, resyncPeriod)

	backupRepositoryInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.addBackupRepository,
		//UpdateFunc: rc.updateBackupRepository,
		//DeleteFunc: rc.deleteBackupRepository,
	}, resyncPeriod)

	backupRepositoryClaimInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) { ctrl.enqueueBackupRepositoryClaim(obj) },
			//UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueBackupRepositoryClaim(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.dequeBackupRepositoryClaim(obj) },
		},
		resyncPeriod,
	)

	svcBackupRepositoryClaimInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		//AddFunc:    rc.svcAddBackupRepositoryClaim,
		//UpdateFunc: rc.svcUpdateBackupRepositoryClaim,
		//DeleteFunc: rc.svcDeleteBackupRepositoryClaim,
	}, resyncPeriod)

	return ctrl
}

// getKey helps to get the resource name from resource object
func (ctrl *backupDriverController) getKey(obj interface{}) (string, error) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	objKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		ctrl.logger.Errorf("Failed to get key from object: %v", err)
		return "", err
	}
	ctrl.logger.Debugf("getKey: key %s", objKey)
	return objKey, nil
}

// Run starts the controller.
func (ctrl *backupDriverController) Run(
	ctx context.Context, workers int) {
	defer ctrl.pvcQueue.ShutDown()
	defer ctrl.snapshotQueue.ShutDown()
	defer ctrl.cloneFromSnapshotQueue.ShutDown()

	ctrl.logger.Infof("Starting backup driver controller")
	defer ctrl.logger.Infof("Shutting down backup driver controller")

	stopCh := ctx.Done()

	ctrl.logger.Infof("Waiting for caches to sync")
	if !cache.WaitForCacheSync(stopCh, ctrl.pvSynced, ctrl.pvcSynced, ctrl.svcPVCSynced, ctrl.backupRepositorySynced, ctrl.backupRepositoryClaimSynced, ctrl.svcBackupRepositoryClaimSynced, ctrl.snapshotSynced, ctrl.svcSnapshotSynced, ctrl.cloneFromSnapshotSynced, ctrl.svcCloneFromSnapshotSynced) {
		ctrl.logger.Errorf("Cannot sync caches")
		return
	}
	ctrl.logger.Infof("Caches are synced")

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.pvcWorker, 0, stopCh)
		//go wait.Until(ctrl.pvWorker, 0, stopCh)
		//go wait.Until(ctrl.svcPvcWorker, 0, stopCh)
		go wait.Until(ctrl.snapshotWorker, 0, stopCh)
		//go wait.Until(ctrl.svcSnapshotWorker, 0, stopCh)
		go wait.Until(ctrl.cloneFromSnapshotWorker, 0, stopCh)
		//go wait.Until(ctrl.svcCloneFromSnapshotWorker, 0, stopCh)
		go wait.Until(ctrl.backupRepositoryClaimWorker, 0, stopCh)
	}

	<-stopCh
}

// snapshotWorker is the main worker for snapshot request.
func (ctrl *backupDriverController) snapshotWorker() {
	ctrl.logger.Infof("snapshotWorker: Enter snapshotWorker")

	key, quit := ctrl.snapshotQueue.Get()
	if quit {
		return
	}
	defer ctrl.snapshotQueue.Done(key)

	if err := ctrl.syncSnapshotByKey(key.(string)); err != nil {
		// Put snapshot back to the queue so that we can retry later.
		ctrl.snapshotQueue.AddRateLimited(key)
	} else {
		ctrl.snapshotQueue.Forget(key)
	}
}

// syncSnapshotByKey processes one Snapshot CRD
func (ctrl *backupDriverController) syncSnapshotByKey(key string) error {
	ctrl.logger.Infof("syncSnapshotByKey: Started Snapshot processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("Split meta namespace key of snapshot %s failed: %v", key, err)
		return err
	}

	snapshot, err := ctrl.snapshotLister.Snapshots(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			ctrl.logger.Infof("Snapshot %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		ctrl.logger.Errorf("Get Snapshot %s/%s failed: %v", namespace, name, err)
		return err
	}

	if snapshot.ObjectMeta.DeletionTimestamp == nil {
		ctrl.logger.Infof("syncSnapshotByKey: calling CreateSnapshot %s/%s", snapshot.Namespace, snapshot.Name)
		return ctrl.createSnapshot(snapshot)
	}

	return nil
}

// enqueueSnapshot adds Snapshotto given work queue.
func (ctrl *backupDriverController) enqueueSnapshot(obj interface{}) {
	ctrl.logger.Infof("enqueueSnapshot: %+v", obj)

	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if snapshot, ok := obj.(*backupdriverapi.Snapshot); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(snapshot)
		if err != nil {
			ctrl.logger.Errorf("failed to get key from object: %v, %v", err, snapshot)
			return
		}
		ctrl.logger.Infof("enqueueSnapshot: enqueued %q for sync", objName)
		ctrl.snapshotQueue.Add(objName)
	}
}

func (ctrl *backupDriverController) delSnapshot(obj interface{}) {
	ctrl.logger.Infof("delSnapshot: delete snapshot %v", obj)

	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		return
	}

	snapshot, ok := obj.(*backupdriverapi.Snapshot)
	if !ok || snapshot == nil {
		return
	}

	err = ctrl.deleteSnapshot(snapshot)
	if err != nil {
		ctrl.logger.Errorf("Delete snapshot %s/%s failed: %v", snapshot.Namespace, snapshot.Name, err)
		return
	}

	ctrl.snapshotQueue.Forget(key)
	ctrl.snapshotQueue.Done(key)
}

// pvcWorker is the main worker for PVC request.
func (ctrl *backupDriverController) pvcWorker() {
	ctrl.logger.Infof("pvcWorker: Enter pvcWorker")

	key, quit := ctrl.pvcQueue.Get()
	if quit {
		return
	}
	defer ctrl.pvcQueue.Done(key)

	if err := ctrl.syncPVCByKey(key.(string)); err != nil {
		// Put PVC back to the queue so that we can retry later.
		ctrl.pvcQueue.AddRateLimited(key)
	} else {
		ctrl.pvcQueue.Forget(key)
	}
}

// syncPVCByKey processes one Snapshot CRD
func (ctrl *backupDriverController) syncPVCByKey(key string) error {
	/*ctrl.logger.Infof("syncPVCByKey: Started PVC processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("Split meta namespace key of PVC %s failed: %v", key, err)
		return err
	}

	pvc, err := ctrl.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			ctrl.logger.Infof("PVC %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		ctrl.logger.Errorf("Get PVC %s/%s failed: %v", namespace, name, err)
		return err
	}

	if pvc.ObjectMeta.DeletionTimestamp == nil {
		ctrl.logger.Infof("syncPVCByKey: calling CreatePVC %s/%s", pvc.Namespace, pvc.Name)
		// TODO: Wait for PV to be created and bound with PVC
		// Then pass PV.Spec.VolumeHandle to CreatePVC so that
		// data can be downloaded and copied to PV
		pvName := pvc.Spec.VolumeName
		pv, err := ctrl.pvLister.Get(pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve PV %s from the API server: %q", pvName, err)
		}
		// Verify binding between PV/PVC is valid
		bound := ctrl.isVolumeBoundToClaim(pv, pvc)
		if bound {
			return ctrl.createVolumeFromSnapshot(pvc)
		}
	}*/

	return nil
}

// isVolumeBoundToClaim returns true, if given volume is pre-bound or bound
// to specific claim. Both claim.Name and claim.Namespace must be equal.
// If claim.UID is present in volume.Spec.ClaimRef, it must be equal too.
func (ctrl *backupDriverController) isVolumeBoundToClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) bool {
	if volume.Spec.ClaimRef == nil {
		return false
	}
	if claim.Name != volume.Spec.ClaimRef.Name || claim.Namespace != volume.Spec.ClaimRef.Namespace {
		return false
	}
	if volume.Spec.ClaimRef.UID != "" && claim.UID != volume.Spec.ClaimRef.UID {
		return false
	}
	return true
}

// enqueuePVC adds PVC given work queue.
func (ctrl *backupDriverController) enqueuePVC(obj interface{}) {
	ctrl.logger.Infof("enqueuePVC: %+v", obj)

	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if brc, ok := obj.(*v1.PersistentVolumeClaim); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(brc)
		if err != nil {
			ctrl.logger.Errorf("failed to get key from object: %v, %v", err, brc)
			return
		}
		ctrl.logger.Infof("enqueuePVC: enqueued %q for sync", objName)
		ctrl.pvcQueue.Add(objName)
	}
}

func (ctrl *backupDriverController) svcPvcWorker() {
}

func (ctrl *backupDriverController) pvWorker() {
}

func (ctrl *backupDriverController) svcSnapshotWorker() {
}

// cloneFromSnapshotWorker is the main worker for restore request.
func (ctrl *backupDriverController) cloneFromSnapshotWorker() {
	ctrl.logger.Infof("cloneFromSnapshotWorker: Enter cloneFromSnapshotWorker")

	key, quit := ctrl.cloneFromSnapshotQueue.Get()
	if quit {
		return
	}
	defer ctrl.cloneFromSnapshotQueue.Done(key)

	if err := ctrl.syncCloneFromSnapshotByKey(key.(string)); err != nil {
		// Put cloneFromSnapshot back to the queue so that we can retry later.
		ctrl.cloneFromSnapshotQueue.AddRateLimited(key)
	} else {
		ctrl.cloneFromSnapshotQueue.Forget(key)
	}
}

// syncCloneFromSnapshotByKey processes one CloneFromSnapshot CRD
func (ctrl *backupDriverController) syncCloneFromSnapshotByKey(key string) error {
	ctrl.logger.Infof("syncCloneFromSnapshotByKey: Started CloneFromSnapshot processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("Split meta namespace key of CloneFromSnapshot %s failed: %v", key, err)
		return err
	}

	cloneFromSnapshot, err := ctrl.cloneFromSnapshotLister.CloneFromSnapshots(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			ctrl.logger.Infof("CloneFromSnapshot %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		ctrl.logger.Errorf("Get CloneFromSnapshot %s/%s failed: %v", namespace, name, err)
		return err
	}

	if cloneFromSnapshot.ObjectMeta.DeletionTimestamp == nil {
		ctrl.logger.Infof("syncCloneFromSnapshotByKey: calling CloneFromSnapshot %s/%s", cloneFromSnapshot.Namespace, cloneFromSnapshot.Name)
		//volumeID
		_, err := ctrl.createVolumeFromSnapshot(cloneFromSnapshot)
		if err != nil {
			ctrl.logger.Errorf("createVolumeFromSnapshot %s/%s failed: %v", namespace, name, err)
			return err
		}
		/*
			//return ctrl.cloneFromSnapshot(CloneFromSnapshot)
			// TODO: create PVC dynamically
			// Construct a PVC
			// TODO: Question? Find to find the original PVC with StorageClass
			// Is it inside the CloneFromSnapshotSpec.Metadata?

			// TODO: Check if Guest Cluster.  If so, create CloneFromSnapshot in Supervisor

			if ctrl.svcKubeConfig != nil {
				// This indicates we are in the Guest Cluster
				// Create CloneFromSnapshot in Supervisor
			}
			/*type PersistentVolumeClaimSpec struct {
			  // Contains the types of access modes required
			  // +optional
			  AccessModes []PersistentVolumeAccessMode
			  // A label query over volumes to consider for binding. This selector is
			  // ignored when VolumeName is set
			  // +optional
			  Selector *metav1.LabelSelector
			  // Resources represents the minimum resources required
			  // +optional
			  Resources ResourceRequirements
			  // VolumeName is the binding reference to the PersistentVolume backing this
			  // claim. When set to non-empty value Selector is not evaluated
			  // +optional
			  VolumeName string
			  // Name of the StorageClass required by the claim.
			  // More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
			  // +optional
			  StorageClassName *string
			  // volumeMode defines what type of volume is required by the claim.
			  // Value of Filesystem is implied when not included in claim spec.
			  // +optional
			  VolumeMode *PersistentVolumeMode*/
		// Metadata []byte `json:"metadata,omitempty"`

		/*pvcName := cloneFromSnapshot.Metadata["Name"]
		accessModes := cloneFromSnapshot.Metadata["AccessModes"]
		resources := cloneFromSnapshot.Metadata["Resources"]
		storageClassName := cloneFromSnapshot.Metadata["StorageClassName"]
		volumeMode := cloneFromSnapshot.Metadata["VolumeMode"]
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: cloneFromSnapshot.Namespace,
				Name:      pvcName,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes:      accessModes,
				Resources:        resources,
				StorageClassName: storageClassName,
				VolumeMode:       volumeMode,
			},
		}

		if newPVC, err = kubeClient.CoreV1().PersistentVolumeClaims(cloneFromSnapshot.Namespace).Create(pvc); err == nil || apierrs.IsAlreadyExists(err) {
			// Save succeeded.
			if err != nil {
				klog.V(3).Infof("PVC %s/%s already exists, reusing", pvc.Namespace, pvc.Name)
				err = nil
			} else {
				klog.V(3).Infof("PVC %s/%s saved", pvc.Namespace, pvc.Name)
			}
		}*/
	}

	return nil
}

// enqueueCloneFromSnapshot adds CloneFromSnapshotto given work queue.
func (ctrl *backupDriverController) enqueueCloneFromSnapshot(obj interface{}) {
	ctrl.logger.Infof("enqueueCloneFromSnapshot: %+v", obj)

	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if cloneFromSnapshot, ok := obj.(*backupdriverapi.CloneFromSnapshot); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(cloneFromSnapshot)
		if err != nil {
			ctrl.logger.Errorf("failed to get key from object: %v, %v", err, cloneFromSnapshot)
			return
		}
		ctrl.logger.Infof("enqueueCloneFromSnapshot: enqueued %q for sync", objName)
		ctrl.cloneFromSnapshotQueue.Add(objName)
	}
}

func (ctrl *backupDriverController) svcCloneFromSnapshotWorker() {
}

func (ctrl *backupDriverController) backupRepositoryClaimWorker() {
	ctrl.logger.Debugf("backupRepositoryClaim: Enter backupRepositoryClaimWorker")

	key, quit := ctrl.backupRepositoryClaimQueue.Get()
	if quit {
		return
	}
	defer ctrl.backupRepositoryClaimQueue.Done(key)

	if err := ctrl.syncBackupRepositoryClaimByKey(key.(string)); err != nil {
		// Put backuprepositoryclaim back to the queue so that we can retry later.
		ctrl.backupRepositoryClaimQueue.AddRateLimited(key)
	} else {
		ctrl.backupRepositoryClaimQueue.Forget(key)
	}

}

// syncBackupRepositoryClaim processes one BackupRepositoryClaim CRD
func (ctrl *backupDriverController) syncBackupRepositoryClaimByKey(key string) error {
	ctrl.logger.Infof("syncBackupRepositoryClaimByKey: Started BackupRepositoryClaim processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		ctrl.logger.Errorf("Split meta namespace key of backupRepositoryClaim %s failed: %v", key, err)
		return err
	}

	brc, err := ctrl.backupRepositoryClaimLister.BackupRepositoryClaims(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			ctrl.logger.Infof("syncBackupRepositoryClaimByKey: BackupRepositoryClaim %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		ctrl.logger.Errorf("Get BackupRepositoryClaim %s/%s failed: %v", namespace, name, err)
		return err
	}

	if brc.ObjectMeta.DeletionTimestamp == nil {
		ctx := context.Background()
		var svcBackupRepositoryName string
		// In case of guest clusters, create BackupRepositoryClaim in the supervisor namespace
		if ctrl.svcKubeConfig != nil {
			svcBackupRepositoryName, err = ClaimSvcBackupRepository(ctx, brc, ctrl.svcKubeConfig, ctrl.supervisorNamespace, ctrl.logger)
			if err != nil {
				ctrl.logger.Errorf("Failed to create Supervisor BackupRepositoryClaim")
				return err
			}
			ctrl.logger.Infof("Created Supervisor BackupRepositoryClaim with BackupRepository %s", svcBackupRepositoryName)
		}

		// Create BackupRepository when a new BackupRepositoryClaim is added
		// Save the supervisor backup repository name to be passed to snapshot manager
		ctrl.logger.Infof("syncBackupRepositoryClaimByKey: Create BackupRepository for BackupRepositoryClaim %s/%s", brc.Namespace, brc.Name)
		// Create BackupRepository when a new BackupRepositoryClaim is added and if the BackupRepository is not already created
		br, err := CreateBackupRepository(ctx, brc, svcBackupRepositoryName, ctrl.backupdriverClient, ctrl.logger)
		if err != nil {
			ctrl.logger.Errorf("Failed to create BackupRepository")
			return err
		}

		err = PatchBackupRepositoryClaim(brc, br.Name, brc.Namespace, ctrl.backupdriverClient)
		if err != nil {
			return err
		}
	}

	return nil
}

// enqueueBackupRepositoryClaimWork adds BackupRepositoryClaim to given work queue.
func (ctrl *backupDriverController) enqueueBackupRepositoryClaim(obj interface{}) {
	ctrl.logger.Debugf("enqueueBackupRepositoryClaim: %+v", obj)

	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if brc, ok := obj.(*backupdriverapi.BackupRepositoryClaim); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(brc)
		if err != nil {
			ctrl.logger.Errorf("failed to get key from object: %v, %v", err, brc)
			return
		}
		ctrl.logger.Infof("enqueueBackupRepositoryClaim: enqueued %q for sync", objName)
		ctrl.backupRepositoryClaimQueue.Add(objName)
	}
}

func (ctrl *backupDriverController) dequeBackupRepositoryClaim(obj interface{}) {
	ctrl.logger.Debugf("dequeBackupRepositoryClaim: Remove BackupRepositoryClaim %v from queue", obj)

	var key string
	var err error
	if key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err != nil {
		return
	}

	brc, ok := obj.(*backupdriverapi.BackupRepositoryClaim)
	if !ok || brc == nil {
		return
	}
	// Delete BackupRepository from API server
	if brc.BackupRepository != "" {
		ctrl.logger.Infof("dequeBackupRepositoryClaim: Delete BackupRepository %s for BackupRepositoryClaim %s/%s", brc.BackupRepository, brc.Namespace, brc.Name)
		err = ctrl.backupdriverClient.BackupRepositories().Delete(brc.BackupRepository, &metav1.DeleteOptions{})
		if err != nil && !k8serrors.IsNotFound(err) {
			ctrl.logger.Errorf("Delete BackupRepository %s failed: %v", brc.BackupRepository, err)
			return
		}
	}

	ctrl.backupRepositoryClaimQueue.Forget(key)
	ctrl.backupRepositoryClaimQueue.Done(key)
}
