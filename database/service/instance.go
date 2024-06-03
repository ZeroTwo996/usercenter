package service

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"usercenter/database"
	"usercenter/database/model"
)

var loginMutex sync.Mutex

// 获取可用实例并接入终端
func GetInstanceAndLogin(zoneID string, siteID string, deviceID string) (*model.Instance, error) {

	loginMutex.Lock()
	defer loginMutex.Unlock()

	// 获取边缘可用的实例
	instance, err := getAvailableInstanceFromSite(zoneID, siteID)
	if err == nil {
		// 边缘有可用实例
		instance, err := updateInstanceStatus(instance, deviceID, "using")
		if err != nil {
			return nil, fmt.Errorf("failed to update instance information in %s: %v", siteID, err)
		}
		return instance, nil
	}

	// 获取中心可用实例
	instance, err = getAvailableInstanceFromCenter(zoneID)
	if err == nil {
		// 中心有可用实例
		instance.SiteID = siteID // 弹性实例需要额外给site_id赋值
		instance, err := updateInstanceStatus(instance, deviceID, "using")
		if err != nil {
			return nil, fmt.Errorf("failed to update instance information in %s: %v", zoneID, err)
		}
		return instance, nil
	}

	return nil, fmt.Errorf("no available instance to be found: %v", err)
}

// 根据终端id更新实例信息
func UpdateInstanceWithDeviceId(ZoneID string, deviceID string) error {
	isElastic := -1 // 初始值，避免未使用的错误

	err := database.DB.QueryRow(fmt.Sprintf(`SELECT is_elastic FROM instance_%s WHERE device_id = ? LIMIT 1`, ZoneID), deviceID).Scan(&isElastic)
	if err != nil {
		return fmt.Errorf("%s cannot be found in %s table: %v", deviceID, ZoneID, err)
	}

	var updateStmt string
	if isElastic == 1 { // 如果是弹性实例就需要修改site_id为null
		updateStmt = fmt.Sprintf(`UPDATE instance_%s SET site_id = 'null', status = 'available', device_id = 'null' WHERE device_id = ?`, ZoneID)
	} else { // 否则就不需要
		updateStmt = fmt.Sprintf(`UPDATE instance_%s SET status = 'available', device_id = 'null' WHERE device_id = ?`, ZoneID)
	}

	_, err = database.DB.Exec(updateStmt, deviceID)
	if err != nil {
		return fmt.Errorf("failed to update instance information when %s logged out from %s: %v", deviceID, ZoneID, err)
	}
	return nil
}

func GetZoneListInDB() (map[string][]string, error) {
	rows, err := database.DB.Query("SHOW TABLES LIKE 'instance_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	zoneList := make(map[string][]string)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		ZoneID := strings.TrimPrefix(tableName, "instance_")

		sites, err := GetSiteListInZone(ZoneID)
		if err != nil {
			log.Printf("Error getting unique site IDs for %s: %v", tableName, err)
			continue
		}
		zoneList[ZoneID] = sites
	}

	fmt.Println(zoneList)
	return zoneList, nil
}

func GetSiteListInZone(ZoneID string) ([]string, error) {
	rows, err := database.DB.Query(fmt.Sprintf("SELECT DISTINCT site_id FROM instance_%s", ZoneID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var siteList []string
	for rows.Next() {
		var siteID string
		if err := rows.Scan(&siteID); err != nil {
			return nil, err
		}
		siteList = append(siteList, siteID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return siteList, nil
}

func getAvailableInstanceFromSite(ZoneID string, siteID string) (*model.Instance, error) {
	instance := &model.Instance{ZoneID: ZoneID}

	query := `SELECT * FROM instance_%s WHERE site_id = ? AND is_elastic = 0 AND status = 'available' ORDER BY RAND() LIMIT 1`
	stmt, err := database.DB.Prepare(fmt.Sprintf(query, ZoneID))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var ignoreId int
	err = stmt.QueryRow(siteID).Scan(&ignoreId, &instance.SiteID, &instance.ServerIP, &instance.InstanceID, &instance.PodName, &instance.Port, &instance.IsElastic, &instance.Status, &instance.DeviceId)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func getAvailableInstanceFromCenter(zoneID string) (*model.Instance, error) {
	instance := &model.Instance{ZoneID: zoneID}

	query := `SELECT * FROM instance_%s WHERE is_elastic = 1 AND status = 'available' ORDER BY RAND() LIMIT 1`
	stmt, err := database.DB.Prepare(fmt.Sprintf(query, zoneID))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var ignoreId int
	err = stmt.QueryRow().Scan(&ignoreId, &instance.SiteID, &instance.ServerIP, &instance.InstanceID, &instance.PodName, &instance.Port, &instance.IsElastic, &instance.Status, &instance.DeviceId)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

// updateInstanceStatus 更新实例的状态和设备ID
func updateInstanceStatus(instance *model.Instance, deviceID string, status string) (*model.Instance, error) {

	updateStmt := fmt.Sprintf(`UPDATE instance_%s SET status = ?, device_id = ? WHERE instance_id = ?`, instance.ZoneID)
	_, err := database.DB.Exec(updateStmt, status, deviceID, instance.InstanceID)
	if err != nil {
		return nil, err
	}

	instance.Status = status
	instance.DeviceId = deviceID

	return instance, nil
}
