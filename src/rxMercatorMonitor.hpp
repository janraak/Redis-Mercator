#ifndef __RXMERCATORMONITOR_H__
#define __RXMERCATORMONITOR_H__

#include "rxMercator.hpp"

int startClientMonitor();
int startInstanceMonitor();
int stopInstanceMonitor();

char *GetCurrentDateTimeAsString(char *buf);
void PostCommand(const char *cmd);

#endif