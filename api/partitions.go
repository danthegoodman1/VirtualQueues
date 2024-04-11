package api

import (
	"github.com/labstack/echo/v4"
	"net/http"
)

func (s *HTTPServer) GetPartitionMap(c echo.Context) error {
	partMap := s.gm.GetPartitionsMap()
	return c.JSON(http.StatusOK, partMap)
}
