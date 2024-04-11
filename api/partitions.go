package api

import "github.com/labstack/echo/v4"

func (s *HTTPServer) GetPartitionMap(c echo.Context) error {
	partMap := s.pm
}
