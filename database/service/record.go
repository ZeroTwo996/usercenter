package service

import (
	"fmt"
	"usercenter/database"
)

// RecordCountForSite 查询站点下 status 为 'using' 的实例个数
func RecordCountForSite(zoneID string, siteID string) (int, error) {

	query := fmt.Sprintf("SELECT COUNT(*) FROM instance_%s WHERE site_id = ? AND status = 'using'", zoneID)
	var count int
	err := database.DB.QueryRow(query, siteID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// InsertRecord 插入记录到 records 表
func InsertRecord(zoneID string, siteID string, date string, instances int) error {
	insertQuery := fmt.Sprintf("INSERT INTO record_%s (site_id, date, instances) VALUES (?, ?, ?)", zoneID)
	if _, err := database.DB.Exec(insertQuery, siteID, date, instances); err != nil {
		return err
	}

	return nil
}
