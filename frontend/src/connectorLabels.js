export function connectorLabel(protocol) {
  return {GCS:'Object storage',EMS:'JMS messaging',KAFKA:'Event stream',SHAREPOINT:'Document library',FILE:'Mounted NAS'}[protocol] || protocol;
}
