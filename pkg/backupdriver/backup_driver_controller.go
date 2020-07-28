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
	"errors"
	"fmt"

	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	backupdriverapi "github.com/vmware-tanzu/velero-plugin-for-vsphere/pkg/apis/backupdriver/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// createSnapshot creates a snapshot of the specified volume, and applies any provided
// set of tags to the snapshot.
func (ctrl *backupDriverController) createSnapshot(snapshot *backupdriverapi.Snapshot) error {
	ctrl.logger.Infof("Entering createSnapshot: %s/%s", snapshot.Namespace, snapshot.Name)

	objName := snapshot.Spec.TypedLocalObjectReference.Name
	objKind := snapshot.Spec.TypedLocalObjectReference.Kind
	if objKind != "PersistentVolumeClaim" {
		errMsg := fmt.Sprintf("resourceHandle Kind %s is not supported. Only PersistentVolumeClaim Kind is supported", objKind)
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	// call SnapshotMgr CreateSnapshot API
	peID := astrolabe.NewProtectedEntityIDWithNamespace(objKind, objName, snapshot.Namespace)
	ctrl.logger.Infof("CreateSnapshot: The initial Astrolabe PE ID: %s", peID)
	if ctrl.snapManager == nil {
		errMsg := fmt.Sprintf("snapManager is not initialized.")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	// NOTE: tags is required to call snapManager.CreateSnapshot
	// but it is not really used
	var tags map[string]string

	brName := snapshot.Spec.BackupRepository
	if ctrl.backupdriverClient == nil {
		errMsg := fmt.Sprintf("backupdriverClient is not initialized")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	if ctrl.svcKubeConfig != nil {
		// For guest cluster, get the supervisor backup repository name
		br, err := ctrl.backupdriverClient.BackupRepositories().Get(brName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.WithError(err).Errorf("Failed to get snapshot Backup Repository %s", brName)
		}
		// Update the backup repository name with the Supervisor BR name
		brName = br.SvcBackupRepositoryName
	}

	peID, err := ctrl.snapManager.CreateSnapshotWithBackupRepository(peID, tags, brName)
	if err != nil {
		errMsg := fmt.Sprintf("failed at calling SnapshotManager CreateSnapshot from peID %v", peID)
		ctrl.logger.Error(errMsg)
		return err
	}

	// Construct the snapshotID for cns volume
	snapshotID := peID.String()
	ctrl.logger.Infof("createSnapshot: The snapshotID depends on the Astrolabe PE ID in the format, <peType>:<id>:<snapshotID>, %s", snapshotID)

	// NOTE: Uncomment the code to retrieve snapshot from API server
	// when needed.
	// Retrieve snapshot from API server to make sure it is up to date
	//newSnapshot, err := ctrl.backupdriverClient.Snapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	//if err != nil {
	//	return err
	//}
	//snapshotClone := newSnapshot.DeepCopy()

	snapshotClone := snapshot.DeepCopy()
	// TODO: Should retrieve progress from Upload CR
	// Retrieve Upload CR from API server to make sure it is up to date
	// upload, err := ctrl.backupdriverClient.Uploads(snapshot.Namespace).Get(uploadName, metav1.GetOptions{})
	//if err != nil {
	//      return err
	//}
	snapshotClone.Status.Phase = backupdriverapi.SnapshotPhaseSnapshotted
	snapshotClone.Status.Progress.TotalBytes = 0
	snapshotClone.Status.Progress.BytesDone = 0
	snapshotClone.Status.SnapshotID = snapshotID

	ctx := context.Background()
	pe, err := ctrl.snapManager.Pem.GetProtectedEntity(ctx, peID)
	if err != nil {
		ctrl.logger.WithError(err).Errorf("Failed to get the ProtectedEntity from peID %s", peID.String())
		return err
	}
	peInfo, err := pe.GetInfo(ctx)
	transports := peInfo.GetMetadataTransports()
	transportType := transports[0].GetTransportType()
	ctrl.logger.Infof("Xing: createSnapshot: Transport type: %s Transports: %+v", transportType, transports)
	//func (this DataTransport) GetParam(key string) (string, bool) {
	//	val, ok := this.params[key]
	//	return val, ok
	//}
	//transportParam := transports[0].GetParam()
	//snapshotClone.Status.Metadata = peInfo.GetMetadataTransports()

	snapshot, err = ctrl.backupdriverClient.Snapshots(snapshotClone.Namespace).UpdateStatus(snapshotClone)
	if err != nil {
		return err
	}

	ctrl.logger.Infof("createSnapshot %s/%s completed with snapshotID: %s, phase in status updated to %s", snapshot.Namespace, snapshot.Name, snapshotID, snapshot.Status.Phase)
	return nil
}

// createSnapshot creates a snapshot of the specified volume, and applies any provided
// set of tags to the snapshot.
/*func (ctrl *backupDriverController) createSnapshot(snapshot *backupdriverapi.Snapshot) error {
	ctrl.logger.Infof("Entering createSnapshot: %s/%s", snapshot.Namespace, snapshot.Name)

	objName := snapshot.Spec.TypedLocalObjectReference.Name
	objKind := snapshot.Spec.TypedLocalObjectReference.Kind
	if objKind != "PersistentVolumeClaim" {
		errMsg := fmt.Sprintf("resourceHandle Kind %s is not supported. Only PersistentVolumeClaim Kind is supported", objKind)
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	// call SnapshotMgr CreateSnapshot API
	peID := astrolabe.NewProtectedEntityIDWithNamespace(objKind, objName, snapshot.Namespace)
	ctrl.logger.Infof("CreateSnapshot: The initial Astrolabe PE ID: %s", peID)
	if ctrl.snapManager == nil {
		errMsg := fmt.Sprintf("snapManager is not initialized.")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	// NOTE: tags is required to call snapManager.CreateSnapshot
	// but it is not really used
	var tags map[string]string

	brName := snapshot.Spec.BackupRepository
	if ctrl.backupdriverClient == nil {
		errMsg := fmt.Sprintf("backupdriverClient is not initialized")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	if ctrl.svcKubeConfig != nil {
		// For guest cluster, get the supervisor backup repository name
		br, err := ctrl.backupdriverClient.BackupRepositories().Get(brName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.WithError(err).Errorf("Failed to get snapshot Backup Repository %s", brName)
		}
		// Update the backup repository name with the Supervisor BR name
		brName = br.SvcBackupRepositoryName
	}
	peID, err := ctrl.snapManager.CreateSnapshotWithBackupRepository(peID, tags, brName)
	if err != nil {
		errMsg := fmt.Sprintf("failed at calling SnapshotManager CreateSnapshot from peID %v", peID)
		ctrl.logger.Error(errMsg)
		return err
	}

	// Construct the snapshotID for cns volume
	snapshotID := peID.String()
	ctrl.logger.Infof("createSnapshot: The snapshotID depends on the Astrolabe PE ID in the format, <peType>:<id>:<snapshotID>, %s", snapshotID)

	// NOTE: Uncomment the code to retrieve snapshot from API server
	// when needed.
	// Retrieve snapshot from API server to make sure it is up to date
	//newSnapshot, err := ctrl.backupdriverClient.Snapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	//if err != nil {
	//      return err
	//}
	//snapshotClone := newSnapshot.DeepCopy()

	snapshotClone := snapshot.DeepCopy()
	snapshotClone.Status.Phase = backupdriverapi.SnapshotPhaseSnapshotted
	snapshotClone.Status.Progress.TotalBytes = 0
	snapshotClone.Status.Progress.BytesDone = 0
	snapshotClone.Status.SnapshotID = snapshotID

	ctx := context.Background()
	pe, err := ctrl.snapManager.pem.GetProtectedEntity(ctx, peID)
	if err != nil {
		ctrl.WithError(err).Errorf("Failed to get the ProtectedEntity from peID %s", peID.String())
		return err
	}
	// TODO: Also need metadata from FCD
	snapshotClone.Status.Metadata = pe.metadata
	ctrl.logger.Infof("createSnapshot: Retrieved metadata for snapshot ID %s, Metadata: %+v", snapshotID, pe.metadata)

	snapshot, err = ctrl.backupdriverClient.Snapshots(snapshotClone.Namespace).UpdateStatus(snapshotClone)
	if err != nil {
		return err
	}

	ctrl.logger.Infof("createSnapshot %s/%s completed with snapshotID: %s, phase in status updated to %s", snapshot.Namespace, snapshot.Name, snapshotID, snapshot.Status.Phase)
	return nil
}*/

// deleteSnapshot deletes the specified volume snapshot.
func (ctrl *backupDriverController) deleteSnapshot(snapshot *backupdriverapi.Snapshot) error {
	ctrl.logger.Infof("deleteSnapshot called with snapshot %s/%s", snapshot.Namespace, snapshot.Name)
	if snapshot.Status.SnapshotID == "" {
		errMsg := fmt.Sprintf("snapshotID is required to delete snapshot")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	snapshotID := snapshot.Status.SnapshotID
	ctrl.logger.Infof("Calling Snapshot Manager to delete snapshot with snapshotID %s", snapshotID)
	peID, err := astrolabe.NewProtectedEntityIDFromString(snapshotID)
	if err != nil {
		ctrl.logger.WithError(err).Errorf("Fail to construct new Protected Entity ID from string %s", snapshotID)
		return err
	}

	if ctrl.snapManager == nil {
		errMsg := fmt.Sprintf("snapManager is not initialized.")
		ctrl.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	err = ctrl.snapManager.DeleteSnapshot(peID)
	if err != nil {
		ctrl.logger.WithError(err).Errorf("Failed at calling SnapshotManager DeleteSnapshot for peID %v", peID)
		return err
	}

	ctrl.logger.Infof("deleteSnapshot %s/%s completed with snapshotID: %s", snapshot.Namespace, snapshot.Name, snapshotID)

	return nil
}

// CreateVolumeFromSnapshot creates a new volume from the provided snapshot,
func (ctrl *backupDriverController) createVolumeFromSnapshot(cloneFromSnapshot *backupdriverapi.CloneFromSnapshot) (string, error) {
	ctrl.logger.Infof("CreateVolumeFromSnapshot called with cloneFromSnapshot: %+v", cloneFromSnapshot)
	var returnVolumeID, returnVolumeType string

	var peId, returnPeId astrolabe.ProtectedEntityID
	var err error
	// TODO(xyang): unwrap metadata in CloneFromSnapshot
	pvcName := "pvcName" // cloneFromSnapshot.Spec.Metadata[0]
	pvcNamespace := "pvcNamespace" // cloneFromSnapshot.Spec.Metadata[1]
	//peId, err = astrolabe.NewProtectedEntityIDFromString(snapshotID)
	//peID
	_ = astrolabe.NewProtectedEntityIDWithNamespace("PersistentVolumeClaim", pvcName, pvcNamespace)
	if err != nil {
		ctrl.logger.WithError(err).Errorf("Fail to construct new PE ID for %s/%s", pvcNamespace, pvcName)
		return returnVolumeID, err
	}

	// For Supervisor and Vanilla cluster, call astrolabe to
	// create PVC in the current cluster dynamically
	/*if ctrl.svcKubeConfig == nil {
		peID, err := astrolabe.CreateVolumeWithParams(peID, params)
		if err != nil {
			errMsg := fmt.Sprintf("failed at calling SnapshotManager CreateSnapshot from peID %v", peID)
			ctrl.logger.Error(errMsg)
			return err
		}
	} else { // We are in Guest Cluster
		// Create CloneFromSnapshot in Supervisor
		// For guest cluster, get the supervisor backup repository name
		br, err := ctrl.backupdriverClient.CloneFromSnapshots(supervisornamespace).Create(cloneFromSnapshot, metav1.CreateOptions{})
		if err != nil {
			ctrl.logger.WithError(err).Errorf("Failed to create CloneFromSnapshot %s/%s in Supervisor Cluster", supervisornamespace, cloneFromSnapshot.Name)
		}
		// TODO(xyang): Waiting for PVC to be created in the supervisor namespace; Create PV statically
		// We can wait for this in backup_driver_controller_base
	}*/

	// TODO: Need to provide both snapshotID for download
	// and volumeHandle because FCD is already created
	// CopyFromRepo in dataMover
	// DataManager download CR created by SnapshotManager for IVD download
	// DataManager copies IVD PE from S3 repo for backup repository
	brName := cloneFromSnapshot.Spec.BackupRepository
	if brName != "" && ctrl.svcKubeConfig != nil {
		// For guest cluster, get the supervisor backup repository name
		br, err := ctrl.backupdriverClient.BackupRepositories().Get(brName, metav1.GetOptions{})
		if err != nil {
			ctrl.logger.WithError(err).Errorf("Failed to get snapshot Backup Repository %s", brName)
		}
		// Update the backup repository name with the Supervisor BR name
		brName = br.SvcBackupRepositoryName
	}

	// When DataManager completes, PVC is returned
	var tags map[string]string
	returnPeId, download, err := ctrl.snapManager.CreateVolumeFromSnapshotWithMetadata(peId, tags /*cloneFromSnapshot.Spec.Metadata*/, cloneFromSnapshot.Spec.SnapshotID, brName)
	if err != nil {
		ctrl.logger.WithError(err).Errorf("Failed at calling SnapshotManager CreateVolumeFromSnapshot with peId %v", peId)
		return returnVolumeID, err
	}

	returnVolumeID = returnPeId.GetID()
	returnVolumeType = returnPeId.GetPeType()

	clone := cloneFromSnapshot.DeepCopy()
	// TODO: Should retrieve Status from Download CR, but we don't know
	// Download CR name here; Alternatively we can update CloneFromSnapshot status
	// in snapshotmgr but seems to be the wront place; Or if we change
	// the return parameters to include Download, that would work too
	// Retrieve Download CR from API server to make sure it is up to date
	// download, err := ctrl.backupdriverClient.Uploads(cloneFromSnapshot.Namespace).Get(downloadName, metav1.GetOptions{})
	//if err != nil {
	//      return err
	//}
	clone.Status.Phase = backupdriverapi.ClonePhase(download.Status.Phase) //backupdriverapi.ClonePhaseCompleted
	clone.Status.Message = download.Status.Message
	apiGroup := ""
	clone.Status.ResourceHandle.APIGroup = &apiGroup
	clone.Status.ResourceHandle.Kind = "PersistentVolumeClaim"
	clone.Status.ResourceHandle.Name = returnVolumeID

	clone, err = ctrl.backupdriverClient.CloneFromSnapshots(clone.Namespace).UpdateStatus(clone)
	if err != nil {
		return "", err
	}

	ctrl.logger.Infof("A new volume %s with type being %s was just created from the call of SnapshotManager CreateVolumeFromSnapshot", returnVolumeID, returnVolumeType)

	return returnVolumeID, nil
}
