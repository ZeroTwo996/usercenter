package service

import (
	"fmt"
	"usercenter/config"
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
	return count, err
}

// InsertRecord 插入记录到 records 表
func InsertRecord(zoneID string, siteID string, date string, instances int) error {

	// 1. 查询record表中site的记录条数
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM record_%s WHERE site_id = ?", zoneID)
	var count int
	if err := database.DB.QueryRow(checkQuery, siteID).Scan(&count); err != nil {
		return err
	}

	// 2. 判断记录条数是否超过阈值，如果是，就需要删除最早的一条记录
	if count >= config.MAXHISTORYNUMBER {
		deleteQuery := fmt.Sprintf("DELETE FROM record_%s WHERE site_id = ? ORDER BY date ASC LIMIT 1", zoneID)
		if _, err := database.DB.Exec(deleteQuery, siteID); err != nil {
			return err
		}
	}

	// 3. 插入最新的一条数据
	insertQuery := fmt.Sprintf("INSERT INTO record_%s (site_id, date, instances) VALUES (?, ?, ?)", zoneID)
	if _, err := database.DB.Exec(insertQuery, siteID, date, instances); err != nil {
		return err
	}

	return nil
}
